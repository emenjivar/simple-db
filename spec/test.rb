describe 'database' do
  def run_script(commands) 
    raw_output = nil
    IO.popen("./main", "r+") do |pipe|
      commands.each do |command|
        pipe.puts command
      end

      pipe.close_write

      # Read entire output
      raw_output = pipe.gets(nil)
    end
    raw_output.split("\n")
  end

  it 'exit from database' do
    result = run_script([
      ".exit",
    ])
    expect(result).to match_array([
      "db > bye",
    ])
  end
end