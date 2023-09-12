defmodule CacheTest do
#  @moduletag timeout: 60_000

  use ExUnit.Case
  doctest Cache

  require Logger

  setup do
    Cache.Application.start([], [])
    context_info = 1
    more_info = 2
    %{context_info: context_info, more_info: more_info}
  end

  test "app tree and infrastructure" do
    [{Setter.TaskSupervisor, _, :supervisor, [Task.Supervisor]},
     {Cache, _, :worker, [Cache]}] = Supervisor.which_children(Cache.Supervisor)
    %{named_table: true, protection: :public} =
      Map.new(:ets.info(Cache.storage_name()))
    assert [] == :ets.tab2list(Cache.storage_name())
  end

  test "get unregistered" do
    assert {:error, :not_registered} == Cache.get("nokey", 1)
  end

  test "simple register and read back (no value, timeout, raw read of value)" do
    ms0 = System.system_time(:millisecond)
    value = 14
    sleep = 500
    f = create_fn(sleep, {:ok, value}, false)
    key = "ok1_5_3_10"
    ttl = 5_000
    refresh = 3_000
    assert :ok == Cache.register_function(f, key, ttl, refresh)
    [{^key, pid1, ^f, :undefined, ^ttl, ^refresh, :undefined}] =
      :ets.lookup(Cache.storage_name(), key)
    assert is_pid(pid1)
    assert {:error, :timeout} == Cache.get(key, 1)
    # value should be ready
    wait(sleep + 250)
    check_ = :ets.lookup(Cache.storage_name(), key)
    [{^key, :undefined, ^f, timestamp, ^ttl, ^refresh, ^value}] = check_
    ms1 = System.system_time(:millisecond)
    assert is_integer(timestamp) and timestamp >= ms0 and timestamp <= ms1
  end

  test "simple seq register and seq get value" do
    value = 15
    sleep = 500
    f = create_fn(sleep, {:ok, value}, false)
    key = "ok2_5_3_10"
    ttl = 5_000
    refresh = 3_000
    assert :ok == Cache.register_function(f, key, ttl, refresh)
    assert {:ok, value} == Cache.get(key, sleep + 250)
  end

  test "simple async register and async get value" do
    value = 15
    sleep = 500
    f = create_fn(sleep, {:ok, value}, false)
    key = "ok2async_5_3_10"
    ttl = 5_000
    refresh = 3_000
    n = 10
    ids = Enum.map(1..n, &Integer.to_string/1)
    # async register
    for suffix <- ids do
      Task.async(fn -> Cache.register_function(f, key <> "_" <> suffix, ttl, refresh) end)
    end
    |> Task.yield_many
    |> Enum.map(fn {task, result} ->
      case result do
        nil ->
          Task.shutdown(task, :brutal_kill)
          exit(:timeout)
        {:exit, reason} ->
          exit(reason)
        {:ok, result} ->
          assert :ok == result
      end
    end)
    #_ms0 = System.system_time(:millisecond)
    wait(sleep + 250)
    # async get
    for suffix <- ids do
      Task.async(fn -> Cache.get(key <> "_" <> suffix, 1) end)
    end
    |> Task.yield_many
    |> Enum.map(fn {task, result} ->
      case result do
        nil ->
          Task.shutdown(task, :brutal_kill)
          exit(:timeout)
        {:exit, reason} ->
          exit(reason)
        {:ok, result} ->
          assert {:ok, value} == result
      end
    end)
    #_ms1 = System.system_time(:millisecond)
  end

  test "kill immediately and no value ever" do
    value = 16
    sleep = 500
    f = create_fn(sleep, {:ok, value}, false)
    key = "ok3a_5_3_10"
    ttl = 5_000
    refresh = 3_000
    :ok = Cache.register_function(f, key, ttl, refresh)
    [{^key, pid1, _, :undefined, _, _, _}] = :ets.lookup(Cache.storage_name(), key)
    assert Process.exit(pid1, :kill)
    assert {:error, :not_ready} == Cache.get(key, sleep + 250)
  end

  test "kill later and check expire" do
    value = 17
    sleep = 500
    f = create_fn(sleep, {:ok, value}, false)
    key = "ok3b_5_3_10"
    ttl = 5_000
    refresh = 3_000
    :ok = Cache.register_function(f, key, ttl, refresh)
    [{^key, pid1, _, :undefined, _, _, _}] = :ets.lookup(Cache.storage_name(), key)
    assert {:error, :timeout} == Cache.get(key, 1)
    assert {:ok, value} == Cache.get(key, sleep + 250)
    assert {:ok, value} == Cache.get(key, 1)
    # no process
    assert !Process.alive?(pid1)
    check_ = :ets.lookup(Cache.storage_name(), key)
    [{^key, :undefined, _, _, _, _, _}] = check_
    wait(refresh + 100)
    [{^key, pid2, _, _, _, _, _}] = :ets.lookup(Cache.storage_name(), key)
    assert is_pid(pid2)
    Process.exit(pid2, :kill)
    assert {:ok, value} == Cache.get(key, 1)
    wait(ttl)
    assert {:error, :expired} == Cache.get(key, 1)
  end

  test "basic refresh" do
    value = 18
    sleep = 500
    key = "ok4a_5_3_10"
    f = create_inc_fn(sleep, key, value)
    ttl = 5_000
    refresh = 3_000
    :ok = Cache.register_function(f, key, ttl, refresh)
    assert {:ok, value} == Cache.get(key, sleep + 250)
    for n <- 1..3 do
      wait(refresh + sleep + 100)
      assert {:ok, value + n * value} == Cache.get(key, sleep)
    end
  end

  test "basic expire" do
    ttl = 5_000
    refresh = 3_000
    value = 18
    sleep = ttl + 500
    key = "ok4b_5_3_10"
    f = create_inc_fn(sleep, key, value)
    :ok = Cache.register_function(f, key, ttl, refresh)
    assert {:ok, value} == Cache.get(key, sleep + 250)
    for n <- 1..3 do
      wait(refresh + sleep + 100)
      assert {:ok, value + n * value} == Cache.get(key, sleep)
    end
  end

  test "fun fails after 2 ok and value expires" do
    value = 18
    sleep = 500
    key = "ok4c_5_3_10"
    f = create_inc_fail_fn(sleep, key, value)
    ttl = 5_000
    refresh = 3_000
    :ok = Cache.register_function(f, key, ttl, refresh)
    assert {:ok, value} == Cache.get(key, sleep + 250)
    for n <- 1..3 do
      wait(refresh + sleep + 100)
      if n < 3 do
        assert {:ok, value + n * value} == Cache.get(key, sleep)
      else
        wait(ttl + 500)
        assert {:error, :expired} == Cache.get(key, sleep)
      end
    end
  end

  test "function always fails" do
    value = 19
    sleep = 500
    key = "ok5_5_3_10"
    f = create_fn(sleep, {:ok, value}, true)
    ttl = 5_000
    refresh = 3_000
    :ok = Cache.register_function(f, key, ttl, refresh)
    assert {:error, :not_ready} == Cache.get(key, sleep + 250)
  end

  test "wrong function format and double registration" do
    value = 13
    sleep = 500
    f = create_fn(sleep, value, false)
    key = "wrong_format"
    ttl = 5_000
    refresh = 3_000
    assert :ok == Cache.register_function(f, key, ttl, refresh)
    assert {:error, :already_registered} == Cache.register_function(f, key, ttl, refresh)
    [{^key, pid1, ^f, :undefined, ^ttl, ^refresh, :undefined}] =
      :ets.lookup(Cache.storage_name(), key)
    assert is_pid(pid1)
    wait(sleep + 250)
    check_ = :ets.lookup(Cache.storage_name(), key)
    [{^key, :undefined, ^f, :undefined, ^ttl, ^refresh, :undefined}] = check_
  end

  def create_fn(sleep, value, is_fails) do
    fn () ->
      wait(sleep)
      if is_fails do
        throw "fail"
      else
        value
      end
    end
  end

  def create_inc_fn(sleep, key, init_value) do
    fn () ->
      wait(sleep)
      [record] = :ets.lookup(Cache.storage_name(), key)
      r = case elem(record, tuple_size(record) - 1) do
        :undefined ->
          init_value
        db_val ->
          db_val + init_value
      end
      {:ok, r}
    end
  end

  def create_inc_fail_fn(sleep, key, init_value) do
    fn () ->
      wait(sleep)
      [record] = :ets.lookup(Cache.storage_name(), key)
      r = case elem(record, tuple_size(record) - 1) do
        :undefined ->
          init_value
        db_val when db_val > 2 * init_value ->
          throw "inc fail"
        db_val ->
          db_val + init_value
      end
      {:ok, r}
    end
  end

  def wait(t) do
    receive do
    after
      t -> :ok
    end
  end

end
