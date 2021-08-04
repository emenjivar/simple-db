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

  it 'select from empty table' do
    result = run_script([
      "select",
      ".exit"
    ])
    expect(result).to match_array([
      "Executed.",
      "db > Empty table.",
      "db > bye"
    ])
  end
end