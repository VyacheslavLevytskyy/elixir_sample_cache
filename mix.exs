defmodule Cache.MixProject do
  use Mix.Project

  def project do
    [
      app: :cache,
      version: "1.0.0",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      dialyzer: [plt_add_deps: :apps_direct]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Cache.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:dialyxir, "~> 1.3", only: [:dev], runtime: false},
    ]
  end
end
