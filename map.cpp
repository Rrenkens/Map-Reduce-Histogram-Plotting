#include <iostream>
#include <sstream>

int main() {
  std::string line;
  while (getline(std::cin, line)) {
    std::stringstream stream(line);
    std::string key;

    std::getline(stream, key, '\t');

    std::cout << key << "\t1\n";
  }
  return 0;
}
