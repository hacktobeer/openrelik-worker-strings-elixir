defmodule OpenrelikWorkerStrings.MixProject do
  use Mix.Project

  @version "0.2.1"

  def project do
    [
      app: :openrelik_worker_strings,
      version: @version,
      elixir: "~> 1.20",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Extract strings from files (Elixir port of openrelik-worker-strings)"
    ]
  end

  def application do
    [
      extra_applications: [:logger, :crypto],
      mod: {OpenrelikWorkerStrings.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:celery, "~> 0.1.3"},
      {:openrelik_worker_common, "~> 0.1.0"}
    ]
  end
end
