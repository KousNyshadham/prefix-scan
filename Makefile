CXX = g++
CXXFLAGS = -O3 -pthread

pfxsum: main.cpp
	$(CXX) $(CXXFLAGS) -o pfxsum main.cpp
