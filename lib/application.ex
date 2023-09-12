defmodule Cache.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      %{
        id: Cache,
        start: {Cache, :start_link, [[name: Cache]]}
      },
      {Task.Supervisor, name: Setter.TaskSupervisor}
    ]

    opts = [strategy: :one_for_one, name: Cache.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
