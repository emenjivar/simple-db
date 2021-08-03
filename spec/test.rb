describe 'database' do
    before do
        `rm -rf test.db`
      end
    
      def run_script(commands)
        raw_output = nil
        IO.popen("./main test.db", "r+") do |pipe|
          commands.each do |command|
            begin
              pipe.puts command
            rescue Errno::EPIPE
              break
            end
          end
    
          pipe.close_write
    
          # Read entire output
          raw_output = pipe.gets(nil)
        end
        raw_output.split("\n")
      end
    
      it 'inserts and retrieves a row' do
        result = run_script([
          ".exit",
        ])
        expect(result).to match_array([
          "db > bye",
        ])
      end
end