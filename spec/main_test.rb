describe 'database' do
  before do 
    `rm -rf db/__ruby_db_*`
  end

  after do 
    `rm -rf db/__ruby_db_*`
  end

  def run_script(db_name, commands) 
    raw_output = nil
    IO.popen("./main __ruby_db_#{db_name}", "r+") do |pipe|
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
    result = run_script("test_exit", [
      ".exit",
    ])
    expect(result).to match_array([
      "db > Bye.",
    ])
  end

  it 'select from empty table' do
    result = run_script("test_empty_table", [
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
    result = run_script("test_insert_select", [
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
    result = run_script("test_table_full", script)
    expect(result[-2]).to eq('db > Error: Table full.')
  end

  it 'verify when string are the maximun length' do
    long_username = "a"*32
    long_email = "a"*255

    result = run_script("test_max_length", [
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

    result = run_script("test_string_too_long", [
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
    result = run_script("test_negative_id", [
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

  it 'verify invalid database name' do 
    result = run_script("../test_invalid_db_name",[]);
    result.push(run_script("/",[])[0]);
    result.push(run_script("#",[])[0]);
    result.push(run_script("=",[])[0]);
    result.push(run_script("db.exe",[])[0]);

    expect(result).to match_array([
      "Invalid database name.",
      "Invalid database name.",
      "Invalid database name.",
      "Invalid database name.",
      "Invalid database name."
    ])
  end

  it 'verify when data is saved after closing connection' do
    db_name = "test_saved_data"

    result_one = run_script(db_name, [
      "insert 1 user user@gmail.com",
      ".exit"
    ])

    expect(result_one).to match_array([
      "db > Executed.",
      "db > Bye."
    ])

    result_two = run_script(db_name, [
      "select",
      ".exit"
    ])

    expect(result_two).to match_array([
      "db > (1, user, user@gmail.com)",
      "1 rows printed.",
      "Executed.",
      "db > Bye."
    ])
  end
end