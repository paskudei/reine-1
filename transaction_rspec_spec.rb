require_relative 'transaction_processor'

RSpec.describe TransactionProcessor do
  subject(:processor) { described_class.new(input_filepath:, output_filepath:) }

  describe '#call' do
    subject(:processor_call) { processor.call }

    let(:input_filepath) { 'transactions.txt' }
    let(:output_filepath) { 'result.txt' }
    let(:output_file) { File.readlines(output_filepath).map(&:chomp) }
    let(:expected_result) do
      [
        '2023-09-03T13:15:00Z,txn12347,user989,900.05',
        '2023-09-03T13:15:00Z,txn12347,user989,700.03',
        '2023-09-03T13:15:00Z,txn12347,user989,600.04',
        '2023-09-03T12:45:00Z,txn12345,user987,500.01',
        '2023-09-03T13:00:00Z,txn12346,user988,300.02',
        '2023-09-03T13:15:00Z,txn12347,user989,100.06'
      ]
    end

    before do
      File.write(input_filepath, <<~TEXT)
        2023-09-03T12:45:00Z,txn12345,user987,500.01
        2023-09-03T13:00:00Z,txn12346,user988,300.02
        2023-09-03T13:15:00Z,txn12347,user989,700.03
        2023-09-03T13:15:00Z,txn12347,user989,600.04
        2023-09-03T13:15:00Z,txn12347,user989,900.05
        2023-09-03T13:15:00Z,txn12347,user989,100.06
      TEXT
      processor_call
    end

    after do
      File.delete(input_filepath) if File.exist?(input_filepath)
      File.delete(output_filepath) if File.exist?(output_filepath)
    end

    it { expect(output_file).to eq(expected_result) }
  end
end
