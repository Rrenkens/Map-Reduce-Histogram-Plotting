#include <iostream>
#include <string>
#include <sstream>

int main() {
  std::string line, key, value;
  size_t count = 0;
  while (std::getline(std::cin, line)) {
    std::istringstream stream(line);
    std::getline(stream, key, '\t');

    while(!stream.eof()) {
      stream >> value;
      count += std::stoi(value);
    }
  }
  std::cout << key << "\t" << count << std::endl;
  return 0;
}
