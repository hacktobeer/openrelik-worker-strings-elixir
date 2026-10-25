defmodule OpenrelikWorkerStrings.MixProject do
  use Mix.Project

  @version "0.2.1"
  @source_url "https://github.com/hacktobeer/ex_openrelik_worker_common"

  def project do
    [
      app: :openrelik_worker_strings,
      version: @version,
      elixir: "~> 1.20",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Extract strings from files (Elixir port of openrelik-worker-strings)",
      source_url: @source_url,
      package: package()
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

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @source_url,
        "OpenRelik" => "https://openrelik.org"
      }
    ]
  end

  defp deps do
    [
      # Pinned to the 0.1.x line for wire compatibility with the Python Celery
      # stack (task metadata serialization format and celeryev event payloads).
      {:celery, "~> 0.1.3"},
      {:openrelik_worker_common, "~> 0.1.0"}
    ]
  end
end
