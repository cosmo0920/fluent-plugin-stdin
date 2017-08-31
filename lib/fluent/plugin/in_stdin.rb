require 'fluent/plugin/input'

module Fluent::Plugin
  class StdinInput < Input
    Fluent::Plugin.register_input('stdin', self)

    helpers :parser, :compat_parameters, :thread

    config_param :format, :string
    config_param :delimiter, :string, :default => "\n"
    config_param :tag, :string, :default => 'stdin.events'
    config_param :stop_at_finished, :bool, :default => true

    def configure(conf)
      compat_parameters_convert(conf, :parser)
      super

      @parser = parser_create
    end

    def start
      super
      @buffer = "".force_encoding('ASCII-8BIT')
      thread_create(:in_stdin_run, &method(:run))
    end

    def shutdown
      super
    end

    def run
      while true
        begin
          @buffer << $stdin.sysread(4000)
          pos = 0

          while i = @buffer.index(@delimiter, pos)
            msg = @buffer[pos...i]
            emit_event(msg)
            pos = i + @delimiter.length
          end
          @buffer.slice!(0, pos) if pos > 0
        rescue IOError, EOFError => e
          # ignore above exceptions because can't re-open stdin automatically
          break
        rescue => e
          log.error "unexpected error", :error=> e.to_s
          log.error_backtrace
          break
        end
      end
      if @stop_at_finished
        Fluent::Engine.flush!
        sleep 1 # avoid 'process died within 1 second. exit.' log
        Fluent::Engine.stop
      end
    end

    def emit_event(msg)
      @parser.parse(msg) { |time, record|
        unless time && record
          log.warn "pattern not match: #{msg.inspect}"
          return
        end

        router.emit(@tag, time, record)
      }
    rescue => e
      log.error msg.dump, :error => e, :error_class => e.class
      log.error_backtrace
    end
  end
end
