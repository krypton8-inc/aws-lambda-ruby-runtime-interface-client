# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.

# frozen_string_literal: true

require 'net/http'
require 'socket'
require 'openssl'
require 'json'
require_relative 'lambda_errors'

class LambdaServer
  LAMBDA_DEFAULT_SERVER_ADDRESS = '127.0.0.1:9001'
  LAMBDA_RUNTIME_API_VERSION = '2018-06-01'

  MAX_HEADER_SIZE_BYTES = 1024 * 1024
  LONG_TIMEOUT_MS = 1_000_000

  def initialize(server_address, user_agent)
    server_address ||= LAMBDA_DEFAULT_SERVER_ADDRESS
    @server_address = "http://#{server_address}/#{LAMBDA_RUNTIME_API_VERSION}"
    @user_agent = user_agent
  end

  def next_invocation
    next_invocation_uri = URI(@server_address + '/runtime/invocation/next')
    begin
      http = Net::HTTP.new(next_invocation_uri.host, next_invocation_uri.port)
      http.read_timeout = LONG_TIMEOUT_MS
      resp =
        http.start do |connection|
          connection.get(
            next_invocation_uri.path,
            { 'User-Agent' => @user_agent },
          )
        end
      if resp.is_a?(Net::HTTPSuccess)
        request_id = resp['Lambda-Runtime-Aws-Request-Id']
        [request_id, resp]
      else
        raise LambdaErrors::InvocationError.new(
                "Received #{resp.code} when waiting for next invocation.",
              )
      end
    rescue LambdaErrors::InvocationError => e
      raise e
    rescue StandardError => e
      raise LambdaErrors::InvocationError.new(e)
    end
  end

  def send_response(
    request_id:,
    response_object:,
    content_type: 'application/json'
  )
    begin
      host = ENV['AWS_LAMBDA_RUNTIME_API']
      port = 443 # AWS Lambda Runtime API uses HTTPS

      # Establish a TCP connection
      tcp_socket = TCPSocket.new(host, port)

      # Wrap the socket with SSL
      ssl_context = OpenSSL::SSL::SSLContext.new
      ssl_socket = OpenSSL::SSL::SSLSocket.new(tcp_socket, ssl_context)
      ssl_socket.sync_close = true
      ssl_socket.connect

      # Prepare the request
      path =
        "/#{LAMBDA_RUNTIME_API_VERSION}/runtime/invocation/#{request_id}/response"
      headers = {
        'Host' => host,
        'Content-Type' => content_type,
        'Lambda-Runtime-Function-Response-Mode' => 'streaming',
        'Transfer-Encoding' => 'chunked',
        'Connection' => 'close',
        'Trailer' =>
          'Lambda-Runtime-Function-Error-Type, Lambda-Runtime-Function-Error-Body',
        'User-Agent' => @user_agent,
      }

      # Send the request line and headers
      ssl_socket.write("POST #{path} HTTP/1.1\r\n")
      headers.each { |key, value| ssl_socket.write("#{key}: #{value}\r\n") }
      ssl_socket.write("\r\n") # End of headers

      if response_object.respond_to?(:each)
        # Streaming response
        begin
          response_object.each { |chunk| send_chunk(ssl_socket, chunk) }
          send_zero_chunk(ssl_socket) # Indicate end of data
          ssl_socket.close
        rescue StandardError => e
          send_trailer(ssl_socket, e)
          ssl_socket.close
          raise e # Reraise to be handled elsewhere if needed
        end
      else
        # Non-streaming response
        send_chunk(ssl_socket, response_object)
        send_zero_chunk(ssl_socket)
        ssl_socket.close
      end
    rescue StandardError => e
      raise LambdaErrors::LambdaRuntimeError.new(e)
    end
  end

  def send_error_response(request_id:, error_object:, error:, xray_cause:)
    response_uri =
      URI(@server_address + "/runtime/invocation/#{request_id}/error")
    begin
      headers = {
        'Lambda-Runtime-Function-Error-Type' => error.runtime_error_type,
        'User-Agent' => @user_agent,
      }
      headers['Lambda-Runtime-Function-XRay-Error-Cause'] =
        xray_cause if xray_cause.bytesize < MAX_HEADER_SIZE_BYTES
      Net::HTTP.post(response_uri, error_object.to_json, headers)
    rescue StandardError => e
      raise LambdaErrors::LambdaRuntimeError.new(e)
    end
  end

  def send_init_error(error_object:, error:)
    uri = URI("#{@server_address}/runtime/init/error")
    begin
      Net::HTTP.post(
        uri,
        error_object.to_json,
        {
          'Lambda-Runtime-Function-Error-Type' => error.runtime_error_type,
          'User-Agent' => @user_agent,
        },
      )
    rescue StandardError => e
      raise LambdaErrors::LambdaRuntimeInitError.new(e)
    end
  end

  private

  def send_chunk(socket, data)
    data = data.to_s
    chunk_size = data.bytesize.to_s(16)
    socket.write("#{chunk_size}\r\n")
    socket.write("#{data}\r\n")
  end

  def send_zero_chunk(socket)
    socket.write("0\r\n\r\n")
  end

  def send_trailer(socket, error)
    # Prepare error details
    error_type = 'Unhandled' # Customize as needed
    error_body = Base64.strict_encode64(error.message)

    # Send zero-length chunk with trailers
    socket.write("0\r\n")
    socket.write("Lambda-Runtime-Function-Error-Type: #{error_type}\r\n")
    socket.write("Lambda-Runtime-Function-Error-Body: #{error_body}\r\n")
    socket.write("\r\n")
  end
end
