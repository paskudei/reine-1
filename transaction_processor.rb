# frozen_string_literal: true

require 'tempfile'

class TransactionProcessor
  attr_reader :input_filepath, :output_filepath

  def initialize(input_filepath:, output_filepath:)
    @input_filepath = input_filepath
    @output_filepath = output_filepath
  end

  def call
    temp_file = Tempfile.new('transaction_chunks')

    File.open(input_filepath, 'r') do |input_file|
      chunk = []
      input_file.each_line do |line|
        chunk << parse_transaction(line)
        if chunk.size >= 100_000
          write_sorted_chunk(temp_file, chunk)
          chunk = []
        end
      end
      write_sorted_chunk(temp_file, chunk) unless chunk.empty?
    end

    merge_sorted_chunks(temp_file, output_filepath)

    temp_file.unlink
  end

  private

  def parse_transaction(line)
    timestamp, transaction_id, user_id, amount = line.chomp.split(',')
    Transaction.new(timestamp, transaction_id, user_id, amount)
  end

  def write_sorted_chunk(temp_file, chunk)
    sorted_chunk = merge_sort(chunk)
    sorted_chunk.each { |txn| temp_file.puts txn.to_s }
    temp_file.puts '---CHUNK_END---'
  end

  def merge_sort(array)
    return array if array.size <= 1

    mid = array.size / 2
    left = merge_sort(array[0...mid])
    right = merge_sort(array[mid..])

    merge(left, right)
  end

  def merge(left, right)
    sorted = []
    sorted << (left.first.amount >= right.first.amount ? left.shift : right.shift) until left.empty? || right.empty?
    sorted + left + right
  end

  def merge_sorted_chunks(temp_file, output_filepath)
    temp_file.rewind

    chunk_iterators = []
    current_chunk = []
    temp_file.each_line do |line|
      if line.chomp == '---CHUNK_END---'
        chunk_iterators << current_chunk.each
        current_chunk = []
      else
        current_chunk << parse_transaction(line)
      end
    end

    File.open(output_filepath, 'w') do |output_file|
      merge_chunks(chunk_iterators, output_file)
    end
  end

  def merge_chunks(chunk_iterators, output_file)
    current_elements = chunk_iterators.map do |iter|
      iter.next
    rescue StandardError
      nil
    end

    while current_elements.any?
      max_index = current_elements.each_with_index.max_by { |txn, _| txn ? txn.amount : -Float::INFINITY }.last
      max_txn = current_elements[max_index]

      output_file.puts max_txn.to_s

      current_elements[max_index] = begin
        chunk_iterators[max_index].next
      rescue StandardError
        nil
      end
    end
  end

  class Transaction
    attr_accessor :timestamp, :transaction_id, :user_id, :amount

    def initialize(timestamp, transaction_id, user_id, amount)
      @timestamp = timestamp
      @transaction_id = transaction_id
      @user_id = user_id
      @amount = amount.to_f
    end

    def to_s
      "#{timestamp},#{transaction_id},#{user_id},#{amount}"
    end
  end
end
