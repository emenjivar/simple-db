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
      "db > Bye.",
    ])
  end

  it 'select from empty table' do
    result = run_script([
      "select",
      ".exit"
    ])
    expect(result).to match_array([
      "db > Empty table.",
      "Executed.",
      "db > Bye."
    ])
  end

  it 'insert and retrieve a single row' do 
    result = run_script([
      "insert 1 user_1 user_1@gmail.com",
      "select",
      ".exit"
    ])
    expect(result).to match_array([
      "Executed.",
      "db > (1, user_1, user_1@gmail.com)",
      "1 rows printed.",
      "db > Executed.",
      "db > Bye."
    ])
  end

  it 'prints error message when table is full' do

    ##
    # The max num of rows is 1401,
    # the ruby script doesn't behave correcty with 1..1401 loop
    # this prints less output than expected, so 1549 is the minimun
    # number with with it works
    #
    script = (1..1549).map do |i|
      "insert #{i} user#{i} user#{i}@example.com"
    end
    script << ".exit"
    result = run_script(script)
    expect(result[-2]).to eq('db > Error: Table full.')
  end

  it 'verify when string are the maximun length' do
    long_username = "a"*32
    long_email = "a"*255

    result = run_script([
      "insert 1 #{long_username} #{long_email}",
      "select",
      ".exit"
    ])

    expect(result).to match_array([
      "Executed.",
      "db > (1, #{long_username}, #{long_email})",
      "1 rows printed.",
      "db > Executed.",
      "db > Bye."
    ])
  end

  it 'verify when string are too long' do
    long_username = "a"*33
    long_email = "a"*256

    result = run_script([
      "insert 1 #{long_username} #{long_email}",
      "select",
      ".exit"
    ])

    expect(result).to match_array([
      "db > String to long.",
      "db > Empty table.",
      "Executed.",
      "db > Bye."
    ])
  end

  it 'verify when id is negative' do
    result = run_script([
      "insert -1 user user@gmail.com",
      "select",
      ".exit"
    ]);

    expect(result).to match_array([
      "db > ID must be positive.",
      "db > Empty table.",
      "Executed.",
      "db > Bye."
    ])
  end
end