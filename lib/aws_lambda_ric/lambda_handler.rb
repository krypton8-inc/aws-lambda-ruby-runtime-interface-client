# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.

require_relative 'aws_lambda_marshaller'

# frozen_string_literal: true
class LambdaHandler
  attr_reader :handler_file_name, :handler_method_name

  def initialize(env_handler:)
    handler_split = env_handler.split('.')
    if handler_split.size == 2
      @handler_file_name, @handler_method_name = handler_split
    elsif handler_split.size == 3
      @handler_file_name, @handler_class, @handler_method_name = handler_split
    else
      raise ArgumentError.new(
              "Invalid handler #{handler_split}, must be of form FILENAME.METHOD or FILENAME.CLASS.METHOD where FILENAME corresponds with an existing Ruby source file FILENAME.rb, CLASS is an optional module/class namespace and METHOD is a callable method. If using CLASS, METHOD must be a class-level method.",
            )
    end
  end

  def call_handler(request:, context:)
    opts = { event: request, context: context }

    if @handler_class
      response =
        Kernel.const_get(@handler_class).send(@handler_method_name, **opts)
    else
      response = __send__(@handler_method_name, **opts)
    end

    if response.respond_to?(:each)
      content_type = 'text/event-stream'
      response, content_type
    else
      AwsLambda::Marshaller.marshall_response(response)
    end
  rescue NoMethodError => e
    raise LambdaErrors::LambdaHandlerCriticalException.new(e)
  rescue NameError => e
    raise LambdaErrors::LambdaHandlerCriticalException.new(e)
  rescue StandardError => e
    raise LambdaErrors::LambdaHandlerError.new(e)
  rescue Exception => e
    raise LambdaErrors::LambdaHandlerCriticalException.new(e)
  end
end
