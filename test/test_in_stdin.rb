require 'fluent/test'
require 'fluent/test/driver/input'
require 'fluent/plugin/in_stdin'

class StdinInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    r, w = IO.pipe
    $stdin = r
    @writer = w
  end

  def teardown
    $stdin = STDIN
  end

  def create_driver(conf)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::StdinInput).configure(conf)
  end

  def test_configure
    d = create_driver("format none")
    assert_equal 'stdin.events', d.instance.tag
    assert_equal "\n", d.instance.delimiter
  end

  {
    'none' => [
      {'msg' => "tcptest1\n", 'expected' => 'tcptest1'},
      {'msg' => "tcptest2\n", 'expected' => 'tcptest2'},
    ],
    'json' => [
      {'msg' => {'k' => 123, 'message' => 'tcptest1'}.to_json + "\n", 'expected' => 'tcptest1'},
      {'msg' => {'k' => 'tcptest2', 'message' => 456}.to_json + "\n", 'expected' => 456},
    ]
  }.each { |format, test_cases|
    define_method("test_msg_size_#{format}") do
      d = create_driver("format #{format}")
      tests = test_cases

      d.run do
        tests.each { |test|
          @writer.write test['msg']
        }
        @writer.close
        sleep 1
      end

      compare_test_result(d.events, tests)
    end
  }

  def compare_test_result(events, tests)
    assert_equal(2, events.size)
    events.each_index {|i|
      assert_equal(tests[i]['expected'], events[i][2]['message'])
    }
  end
end
