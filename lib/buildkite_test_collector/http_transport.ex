defmodule BuildkiteTestCollector.HttpTransport do
  @moduledoc """
  Handles encoding and transmitting payloads to Buildkite's test analytics API
  via HTTP.
  """

  alias BuildkiteTestCollector.Payload

  @doc """
  Send a `Payload` to Buildkite's test analytics API.
  """
  @spec send(Payload.t()) :: {:ok, map} | {:error, any}
  def send(payload) do
    %URI{
      host: host,
      path: path,
      port: port,
      scheme: scheme
    } = URI.parse(endpoint())

    body = Jason.encode!(payload)

    {:ok, conn} = Mint.HTTP2.connect(String.to_atom(scheme), host, port)

    try do
      {:ok, conn, request_ref} =
        response = Mint.HTTP2.request(conn, "POST", path, headers(), :stream)

      connection_window_size = Mint.HTTP2.get_window_size(conn, :connection)
      request_window_size = Mint.HTTP2.get_window_size(conn, {:request, request_ref})
      IO.inspect(connection_window_size, label: "connection_window_size")
      IO.inspect(request_window_size, label: "request_window_size")
      {chunk, next_chunk} = String.split_at(body, connection_window_size)
      {:ok, conn} = Mint.HTTP2.stream_request_body(conn, request_ref, chunk)
      {:ok, conn} = Mint.HTTP2.stream_request_body(conn, request_ref, next_chunk)
      {:ok, conn} = Mint.HTTP2.stream_request_body(conn, request_ref, :eof)

      receive do
        {:ssl, _, _} = msg ->
          {:ok, conn, responses} = Mint.HTTP2.stream(conn, msg)

        {:tcp, _, _} = msg ->
          {:ok, conn, responses} = Mint.HTTP2.stream(conn, msg)
      end

      case response do
        {:ok, _conn, _request_ref} -> {:ok, %{payload: %{data: [], data_size: 0}}}
        {:error, _conn, error} -> {:error, error}
      end
    after
      Mint.HTTP2.close(conn)
    end
  end

  @doc """
  If the payload contains at least the configured batch size (default 500) test
  results, then send a batch to the API.
  """
  @spec maybe_send_batch(Payload.t()) :: {:ok, Payload.t()} | {:error, any}
  def maybe_send_batch(payload), do: maybe_send_batch(payload, batch_size())

  @doc false
  @spec maybe_send_batch(Payload.t(), pos_integer()) :: {:ok, Payload.t()} | {:error, any}
  def maybe_send_batch(%{data_size: data_size, data: data} = payload, batch_size)
      when data_size >= batch_size do
    {this_batch, next_batch} = Enum.split(data, batch_size)
    payload_to_send = %{payload | data: this_batch, data_size: length(this_batch)}
    payload_to_return = %{payload | data: next_batch, data_size: length(next_batch)}
    IO.inspect(byte_size(Jason.encode!(payload_to_send)), label: "payload_to_send")

    case send(payload_to_send) do
      {:ok, _} -> {:ok, payload_to_return}
      {:error, reason} -> {:error, reason}
    end
  end

  def maybe_send_batch(payload, _batch_size), do: {:ok, payload}

  defp headers,
    do: [
      {"content-type", "application/json"},
      {"authorization", "Token token=\"#{api_key()}\""}
    ]

  defp endpoint,
    do:
      Application.get_env(
        :buildkite_test_collector,
        :api_endpoint,
        "https://analytics-api.buildkite.com/v1/uploads"
      )

  defp api_key, do: Application.fetch_env!(:buildkite_test_collector, :api_key)

  defp batch_size, do: Application.get_env(:buildkite_test_collector, :batch_size, 100)
end
