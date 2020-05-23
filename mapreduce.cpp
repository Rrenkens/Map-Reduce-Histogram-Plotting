#include <exception>
#include <iostream>
#include <map>
#include <queue>
#include <sstream>
#include <string>

#include <boost/process.hpp>
#include <boost/filesystem.hpp>

const size_t SIZE = 10000;
const size_t MAX_COUNT_OF_PROCESS = 10;
const long double EPS = 1e-9;

namespace bp = boost::process;
namespace bf = boost::filesystem;

void CreateProcess(std::vector<bp::child> &all_process,
                   const std::string &script_path,
                   std::ofstream &file_for_close,
                   const std::string &input_path,
                   const std::string &output_path,
                   size_t &count_of_file) {

  file_for_close.close();
  all_process.emplace_back(bp::child(script_path,
                                     bp::std_out > output_path +
                                         std::to_string(count_of_file) +
                                         ".txt",
                                     bp::std_err > stderr,
                                     bp::std_in < input_path +
                                         std::to_string(count_of_file) +
                                         ".txt"));
  ++count_of_file;
}

void WaitingCompletionOfAllProcess(std::vector<bp::child> &all_process) {
  for (auto &process : all_process) {
    std::error_code error_code;
    process.wait(error_code);
    if (error_code.value() != 0) {
      throw std::system_error(error_code);
    }
  }
}

size_t RunMappers(std::ifstream &file_input,
                  std::ofstream &temp_file_input,
                  const std::string &script_path,
                  const std::string &temp_input_file_path,
                  const std::string &temp_output_file_path,
                  std::vector<bp::child> &all_process) {
  size_t cur_count_of_line = 0, count_of_file = 0;

  std::string line;
  while (getline(file_input, line)) {
    if (cur_count_of_line == 0) {
      temp_file_input.open(temp_input_file_path +
          std::to_string(count_of_file) + ".txt");
      if (!temp_file_input.is_open()) {
        throw std::logic_error("Something went wrong while creating the file");
      }
    }

    temp_file_input << line << '\n';
    ++cur_count_of_line;

    if (cur_count_of_line == SIZE) {
      CreateProcess(all_process,
                    script_path,
                    temp_file_input,
                    temp_input_file_path,
                    temp_output_file_path,
                    count_of_file);
      cur_count_of_line = 0;
    }

    if (all_process.size() == MAX_COUNT_OF_PROCESS) {
      WaitingCompletionOfAllProcess(all_process);
      all_process.clear();
    }
  }

  if (temp_file_input.is_open()) {
    CreateProcess(all_process,
                  script_path,
                  temp_file_input,
                  temp_input_file_path,
                  temp_output_file_path,
                  count_of_file);
  }

  return count_of_file;
}

size_t RunReducers(std::ifstream &file_input,
                   std::ofstream &temp_file_input,
                   const std::string &script_path,
                   const std::string &temp_input_file_path,
                   const std::string &temp_output_file_path,
                   std::vector<bp::child> &all_process,
                   size_t count_of_range) {

  size_t count_of_file = 0;
  long double step = 1.0 / count_of_range, cur_range = 0;
  std::string line;

  while (std::getline(file_input, line)) {
    std::stringstream stream(line);
    std::string string_key;
    std::getline(stream, string_key, '\t');

    long double cur_value;
    std::stringstream stream_key(string_key);
    stream_key >> cur_value;

    if (cur_value > cur_range + EPS) {
      if (temp_file_input.is_open()) {
        CreateProcess(all_process,
                      script_path,
                      temp_file_input,
                      temp_input_file_path,
                      temp_output_file_path,
                      count_of_file);
      }

      if (all_process.size() == MAX_COUNT_OF_PROCESS) {
        WaitingCompletionOfAllProcess(all_process);
        all_process.clear();
      }

      while (cur_value > cur_range + EPS) {
        cur_range += step;
      }

      temp_file_input.open(temp_input_file_path +
          std::to_string(count_of_file) + ".txt");
    }
    temp_file_input << line << '\n';
  }

  if (temp_file_input.is_open()) {
    CreateProcess(all_process,
                  script_path,
                  temp_file_input,
                  temp_input_file_path,
                  temp_output_file_path,
                  count_of_file);
  }

  return count_of_file;
}

void WriteSortData(std::vector<std::string> &data,
                   const std::string &temp_file_path,
                   size_t &count_of_file) {

  std::ofstream chunk_output;
  sort(data.begin(), data.end());
  chunk_output.open(temp_file_path + std::to_string(count_of_file) + ".txt");

  if (!chunk_output.is_open()) {
    throw std::logic_error("Something went wrong while creating the file");
  }

  for (const auto &el : data) {
    chunk_output << el << '\n';
  }

  chunk_output.close();
  data.clear();
  ++count_of_file;
}

size_t SplitFile(const std::string &input_file,
                 const std::string &temp_file_path) {
  std::ifstream file_input(input_file);
  size_t cur_count_of_line = 0, count_of_file = 0;

  std::string line;
  std::vector<std::string> data;
  while (getline(file_input, line)) {
    data.push_back(line);
    ++cur_count_of_line;

    if (cur_count_of_line == SIZE) {
      WriteSortData(data, temp_file_path, count_of_file);
      cur_count_of_line = 0;
    }
  }

  if (cur_count_of_line != 0) {
    WriteSortData(data, temp_file_path, count_of_file);
  }

  return count_of_file;
}

void MergeFile(const std::string &output_file,
               const std::string &temp_file_path,
               size_t count_of_file) {
  std::vector<std::ifstream> flow_of_file(count_of_file);
  std::priority_queue<std::pair<std::string, size_t>,
                      std::vector<std::pair<std::string, size_t >>,
                      std::greater<> > heap;
  std::string line;

  for (size_t i = 0; i < count_of_file; ++i) {
    flow_of_file[i] = std::ifstream(temp_file_path +
        std::to_string(i) + ".txt");
    getline(flow_of_file[i], line);
    heap.push({line, i});
  }

  std::ofstream out(output_file);

  while (!heap.empty()) {
    std::pair<std::string, size_t> top = heap.top();
    heap.pop();

    if (flow_of_file[top.second].peek() != EOF) {
      getline(flow_of_file[top.second], line);
      heap.push({line, top.second});
    }
    out << top.first << '\n';
  }

  for (auto &el : flow_of_file) {
    el.close();
  }

  out.close();
}

void ExternalSort(const std::string &input_file,
                  const std::string &temp_file_path) {
  size_t count_of_file = SplitFile(input_file, temp_file_path);
  MergeFile(input_file, temp_file_path, count_of_file);
}

void MergeTempFiles(const std::string &output_path,
                    const std::string &temp_output_file_path,
                    size_t count_of_file) {
  std::ofstream file_output(output_path);
  std::string line;

  for (size_t i = 0; i < count_of_file; ++i) {
    std::ifstream temp_file_input(temp_output_file_path +
        std::to_string(i) + ".txt");

    if (!temp_file_input.is_open()) {
      throw std::logic_error("Something went wrong while opening the file");
    }

    while (getline(temp_file_input, line)) {
      file_output << line << '\n';
    }

    temp_file_input.close();
  }

  file_output.close();
}

int main(int argc, char *argv[]) {

  if (argc != 6) {
    throw std::logic_error("Invalid count of arguments");
  }

  std::string mode = argv[1],
      script_path = argv[2],
      input_path = argv[3],
      output_path = argv[4];
  size_t count_of_range = std::stoi(argv[5]);

  if (mode != "map" && mode != "reduce") {
    throw std::logic_error("Invalid mode");
  }

  const bf::path temp_directory = bf::unique_path();
  bf::path temp_input_file_path = temp_directory,
      temp_output_file_path = temp_directory;
  temp_input_file_path.append("input_"),
      temp_output_file_path.append("output_");

  bf::create_directories(temp_directory);

  std::ofstream temp_file_input;
  std::vector<bp::child> all_process;
  std::string line;
  size_t count_of_file;

  if (mode == "map") {
    std::ifstream file_input(input_path);
    count_of_file = RunMappers(file_input,
                               temp_file_input,
                               script_path,
                               temp_input_file_path.string(),
                               temp_output_file_path.string(),
                               all_process);
    file_input.close();
  } else {
    bf::path temp_sort_file_path = temp_directory;
    temp_sort_file_path.append("sort_");

    ExternalSort(input_path, temp_sort_file_path.string());

    std::ifstream file_input(input_path);

    count_of_file = RunReducers(file_input,
                                temp_file_input,
                                script_path,
                                temp_input_file_path.string(),
                                temp_output_file_path.string(),
                                all_process,
                                count_of_range);
    file_input.close();
  }

  WaitingCompletionOfAllProcess(all_process);
  MergeTempFiles(output_path, temp_output_file_path.string(), count_of_file);

  bf::remove_all(temp_directory);

  return 0;
}
