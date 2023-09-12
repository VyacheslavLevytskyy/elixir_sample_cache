defmodule Cache do
  @cache_registry :cache_registry

  require Logger
  use GenServer

  @type result ::
          {:ok, any()}
          | {:error, :timeout}
          | {:error, :not_registered}
          | {:error, :not_ready}
          | {:error, :expired}

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, [], opts)
  end

  @doc ~s"""
  Registers a function that will be computed periodically to update the cache.

  Arguments:
    - `fun`: a 0-arity function that computes the value and returns either
      `{:ok, value}` or `{:error, reason}`.
    - `key`: associated with the function and is used to retrieve the stored
    value.
    - `ttl` ("time to live"): how long (in milliseconds) the value is stored
      before it is discarded if the value is not refreshed.
    - `refresh_interval`: how often (in milliseconds) the function is
      recomputed and the new value stored. `refresh_interval` must be strictly
      smaller than `ttl`. After the value is refreshed, the `ttl` counter is
      restarted.

  The value is stored only if `{:ok, value}` is returned by `fun`. If `{:error,
  reason}` is returned, the value is not stored and `fun` must be retried on
  the next run.
  """
  @spec register_function(
          fun :: (() -> {:ok, any()} | {:error, any()}),
          key :: any(),
          ttl :: non_neg_integer(),
          refresh_interval :: non_neg_integer()
        ) :: :ok | {:error, :already_registered}
  def register_function(fun, key, ttl, refresh_interval)
      when is_function(fun, 0) and is_integer(ttl) and ttl > 0 and
             is_integer(refresh_interval) and
             refresh_interval < ttl do
    GenServer.call(__MODULE__, {:register, key, fun, refresh_interval, ttl})
  end

  @doc ~s"""
  Get the value associated with `key`.

  Details:
    - If the value for `key` is stored in the cache, the value is returned
      immediately.
    - If a recomputation of the function is in progress, the last stored value
      is returned.
    - If the value for `key` is not stored in the cache but a computation of
      the function associated with this `key` is in progress, wait up to
      `timeout` milliseconds. If the value is computed within this interval,
      the value is returned. If the computation does not finish in this
      interval, `{:error, :timeout}` is returned.
    - If `key` is not associated with any function, return `{:error,
      :not_registered}`
  """
  @spec get(any(), non_neg_integer(), Keyword.t()) :: result
  def get(key, timeout \\ 30_000, _opts \\ []) when is_integer(timeout) and timeout > 0 do
    result = Cache.Store.get(@cache_registry, key)
    case result do
      {tag, _} when tag == :ok or tag == :error ->
        result
      {_, running_pid} when is_pid(running_pid) ->
        # wait for calculations to finish or for the timeout
        ref = Process.monitor(running_pid)
        receive do
          {:DOWN, _, :process, ^running_pid, _} ->
            result = Cache.Store.get(@cache_registry, key)
            case result do
              {:wait_not_ready, _} ->
                Logger.warning("#{key}: no recomputation is running, but there is no value")
                {:error, :not_ready}
              {:wait_expired, _}   ->
                Logger.warning("#{key}: value is expired and no recomputation is running")
                {:error, :expired}
              {tag, _} when tag == :ok or tag == :error ->
                result
            end
        after
          timeout ->
            Process.demonitor(ref)
            {:error, :timeout}
        end
    end
  end

  @impl true
  def init([]) do
    refs = %{}
    store = Cache.Store.create_store(@cache_registry)
    {:ok, {store, refs}}
  end

  @impl true
  def handle_call({:register, key, fun, refresh_interval, ttl}, _from, {store, refs} = state) do
    case Map.has_key?(refs, key) do
      true ->
        {:reply, {:error, :already_registered}, state}
      false ->
        case start_function(store, key, fun, refresh_interval, ttl) do
          :ok ->
            # key -> [] means that we only keep track of keys and don't associate anything with a key
            # intention is to keep a placeholder for further extensions, for exaple, for retry logic
            # per `key` or some periodical management functions wrt `key`
            {:reply, :ok, {store, Map.put(refs, key, [])}}
          :error ->
            {:reply, {:error, :not_registered}, state}
        end
    end
  end

  @impl true
  def handle_call(_, _, state) do
    Logger.warning("unexpected message")
    {:reply, :ok, state}
  end

  #@doc ~s"""
  #  Initializes calculations:
  #  - 
  #"""
  @spec start_function(
          store :: atom(),
          key :: any(),
          fun :: (() -> {:ok, any()} | {:error, any()}),
          refresh_interval :: non_neg_integer(),
          ttl :: non_neg_integer()
        ) :: :ok | :error
  defp start_function(store, key, fun, refresh_interval, ttl) do
    parent = self()
    result = Task.Supervisor.start_child(Setter.TaskSupervisor, fn ->
      Process.monitor(parent)
      receive do
        {^parent, :go} ->
          calculate_store_value(parent, store, key, fun, refresh_interval, 0)
        {^parent, :halt} ->
          # it's strange, but parent GenServer somehow missed that the key is already initialized
          exit(:already_registered)
        {:DOWN, _, :process, ^parent, _} ->
          # parent GenServer exited, ETS is lost, there is nothing we can do
          exit(:parent_down)
      end
    end)
    case result do
      {:ok, running_pid} ->
        enable_calculations(parent, store, key, fun, refresh_interval, ttl, running_pid)
      {:ok, running_pid, _} ->
        enable_calculations(parent, store, key, fun, refresh_interval, ttl, running_pid)
      _ ->
        :error
    end
  end

  @doc ~s"""
    Inserts an initialization record into the storage and enables calucations.
  """
  @spec enable_calculations(
          parent :: pid(),
          store :: atom(),
          key :: any(),
          fun :: (() -> {:ok, any()} | {:error, any()}),
          refresh_interval :: non_neg_integer(),
          ttl :: non_neg_integer(),
          running_pid :: pid()
        ) :: :ok | :error
  def enable_calculations(parent, store, key, fun, refresh_interval, ttl, running_pid) do
    case Cache.Store.init(store, key, fun, refresh_interval, ttl, running_pid) do
        true ->
          send(running_pid, {parent, :go})
          :ok
        false ->
          Logger.warning("the storage already has a record with key: #{key}")
          send(running_pid, {parent, :halt})
          :error
    end
  end

  #@doc ~s"""
  #  A wrapper for a function:
  #  - on the first call (n_call == 0), expect that the correct `pid` in the storage
  #    has already been set
  #  - calculate and catch an error if any
  #  - store if no error happened
  #  - clear self `pid` when finished: update `pid` to :undefined in the storage
  #  - the `ttl` counter is "restarted" by setting a new `timestamp` value in the storage
  #  - plan to calculate again after `refresh_interval` of milliseconds
  #"""
  @spec calculate_store_value(
          parent :: pid(),
          store :: atom(),
          key :: any(),
          fun :: (() -> {:ok, any()} | {:error, any()}),
          refresh_interval :: non_neg_integer(),
          n_call :: non_neg_integer()
        ) :: :ok | :error
  defp calculate_store_value(parent, store, key, fun, refresh_interval, n_call) do
    try do
      if n_call > 0 do
        Cache.Store.set_running(store, key, self())
      end
      case fun.() do
        {:ok, value} ->
          Cache.Store.set(store, key, value)
          :ok
        {:error, _} ->
          Cache.Store.set_not_running(store, key)
          :error
        other ->
          Logger.warning("#{key}: unexpected response format from calculations, expect ok or error: #{other}")
          Cache.Store.set_not_running(store, key)
          :error
      end
    catch
      class, reason ->
        #Logger.warning("#{key}: calculations failed: #{class}:#{reason}", __STACKTRACE__)
        Logger.warning("#{key}: calculations failed: #{class}:#{reason}")
        Cache.Store.set_not_running(store, key)
        :error
    after
      case Process.alive?(parent) do
        true ->
          maybe_tref = :timer.apply_after(refresh_interval, __MODULE__,
                                          :async_calculate_store_value,
                                          [parent, store, key, fun, refresh_interval, n_call])
          case maybe_tref do
            {:ok, _tref} ->
              # optionally, send to the storage to be able to manage created timer
              :ok
            {:error, reason} ->
              Logger.error(~s"""
                #{key}: timer creation failed with reason #{reason},
                a value will never be calculated!
              """)
              # optionally, plan to retry again to avoid breaking a chain of periodical calculations
              # for example, send a message to GenServer that keeps track of keys and
              # implement a retry logic for "orphan" functions without an alive timer
              :error
          end
        false ->
          # parent GenServer exited, ETS is lost, cancel calculations
          :ok
      end
    end
  end

  @doc ~s"""
    Async execution of `calculate_store_value`
  """
  @spec async_calculate_store_value(
          parent :: pid(),
          store :: atom(),
          key :: any(),
          fun :: (() -> {:ok, any()} | {:error, any()}),
          refresh_interval :: non_neg_integer(),
          n_call :: non_neg_integer()
        ) :: :ok
  def async_calculate_store_value(parent, store, key, fun, refresh_interval, n_call) do
    # optionally, check return of the start_child() to be able to retry later,
    # in the same way as when creation of a timer fails
    Task.Supervisor.start_child(Setter.TaskSupervisor, fn ->
      calculate_store_value(parent, store, key, fun, refresh_interval, n_call + 1)
    end)
    :ok
  end

  @doc ~s"""
  Returns storage name
  """
  @spec storage_name() :: atom()
  def storage_name() do
    @cache_registry
  end

end

defmodule Cache.Store do
  @moduledoc """
  Interface with the ETS storage.

  ETS record structure: {key, pid, fun, timestamp, ttl, refresh_period, value}, where
      key             - key (const)
      pid             - pid if a running process is calculating a value, or :undefined otherwise
      fun             - function to call to calculate a value (const)
      timestamp       - linux millisecond when the `value` was created, or :undefined otherwise
      ttl             - TTL as set when registered, milliseconds (const)
      refresh_period  - refresh period as set when registered (const)
      value           - value as returned by the `fun` or :undefined if no value available

  Assumptions:
    - if `timestamp` is `:undefined`, there is no ready value
    - expired condition is checked on read as `timestamp` + `ttl` < `now()`
    - if `pid` is not `:undefined`, a running process is calculating a value
    - `fun` and `refresh_period` fields of the storage are for reference and extension only,
      their values from the storage are not in use anywhere in the code
  """

  @doc ~s"""
    Create public ETS table
  """
  @spec create_store(
          name :: atom()
        ) :: :ets.tid()
  def create_store(name) do
    :ets.new(name, [:named_table, :set, :public, read_concurrency: true])
  end

  @doc ~s"""
    Set a new value
  """
  @spec set(
          store :: atom() | :ets.tid(),
          key :: any(),
          value :: any()
        ) :: any()
  def set(store, key, value) do
    timestamp = System.system_time(:millisecond)
    :ets.update_element(store, key, [{2, :undefined}, {4, timestamp}, {7, value}])
  end

  @doc ~s"""
    Update "running" status: set to "running" along with the pid
  """
  @spec set_running(
          store :: atom() | :ets.tid(),
          key :: any(),
          running_pid :: pid()
        ) :: any()
  def set_running(store, key, running_pid) do
    :ets.update_element(store, key, [{2, running_pid}])
  end

  @doc ~s"""
    Update "running" status: set to "not running"
  """
  @spec set_not_running(
          store :: atom() | :ets.tid(),
          key :: any()
        ) :: any()
  def set_not_running(store, key) do
    :ets.update_element(store, key, [{2, :undefined}])
  end

  @doc ~s"""
    Inserts the first version of a record for a fun
  """
  @spec init(
          store :: atom() | :ets.tid(),
          key :: any(),
          fun :: (() -> {:ok, any()} | {:error, any()}),
          refresh_interval :: non_neg_integer(),
          ttl :: non_neg_integer(),
          running_pid :: pid()
        ) :: boolean()
  def init(store, key, fun, refresh_interval, ttl, running_pid) do
    :ets.insert_new(store, {key, running_pid, fun, :undefined, ttl, refresh_interval, :undefined})
  end

  @doc ~s"""
    Get value, or error, or a signal to wait along with the pid to wait for
  """
  @spec get(
          store :: atom() | :ets.tid(),
          key :: any()
        ) :: {:ok, any()} |
             {:error, :not_registered | :not_ready | :expired} |
             {:wait_not_ready | :wait_expired, pid()}
  def get(store, key) do
    ms = System.system_time(:millisecond)
    case :ets.lookup(store, key) do
      [{_, running_pid, _, :undefined, _, _, _}] when is_pid(running_pid) ->
        # value is not ready, recomputation is in progress
        {:wait_not_ready, running_pid}
      [{_, _, _, :undefined, _, _, _}] ->
        # value is not ready although recomputation must have been completed
        {:error, :not_ready}
      [{_, running_pid, _, timestamp, ttl, _, _}] when is_pid(running_pid) and timestamp + ttl < ms ->
        # expired, recomputation is in progress
        {:wait_expired, running_pid}
      [{_, _, _, timestamp, ttl, _, _}] when timestamp + ttl < ms ->
        # expired, updated value is not available and is not expected (e.g., no recomputations)
        {:error, :expired}
      [{_, _, _, _, _, _, value}] ->
        # ready, a recomputation may be in progress or not
        {:ok, value}
      [] ->
        {:error, :not_registered}
    end
  end
end
