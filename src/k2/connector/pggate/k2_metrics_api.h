/*
MIT License

Copyright(c) 2021 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

#pragma once

#include <string>
#include <vector>

#include "k2_includes.h"
#include "k2_log.h"

extern "C" {

namespace k2pg::metrics {
struct Histogram {
    Histogram(std::string name, std::string help, int start, double factor, int count, std::vector<std::string> labels):
            _name(std::move(name)), _help(std::move(help)), _start(start), _factor(factor), _count(count), _labels(std::move(labels)) {
        K2LOG_D(log::k2Client, "creating histogram metric: {}", _name);
        _labels_c.reserve(_labels.size());
        for (auto& l: _labels) {
            _labels_c.push_back(l.c_str());
        }
    }

    ~Histogram() {
        K2LOG_D(log::k2Client, "deleting histogram metric: {}", _name);
    }

    void observe(double value, const char** label_values=NULL) {
        K2LOG_D(log::k2Client, "observe value {} in histogram metric: {}", value, _name);
    }

    void observe(k2::Duration value, const char** label_values=NULL) {
        K2LOG_D(log::k2Client, "observe duration {} in histogram metric: {}", value, _name);
    }

private:
    std::string _name;
    std::string _help;
    int _start{0};
    double _factor{0};
    int _count{0};
    std::vector<std::string> _labels;
    std::vector<const char*> _labels_c;
};

struct Gauge {
    Gauge(std::string name, std::string help, std::vector<std::string> labels) :
            _name(std::move(name)), _help(std::move(help)), _labels(std::move(labels)) {
        K2LOG_D(log::k2Client, "create gauge metric: {}", _name);
        _labels_c.reserve(_labels.size());
        for (auto& l : _labels) {
            _labels_c.push_back(l.c_str());
        }
    }

    ~Gauge() {
        K2LOG_D(log::k2Client, "destroy gauge metric: {}", _name);
    }

    void set(double value, const char** label_values=NULL) {
        K2LOG_D(log::k2Client, "set gauge metric: {} for name {}", value, _name);
    }

    void add(double value, const char** label_values=NULL) {
        K2LOG_D(log::k2Client, "add gauge metric: {} for name {}", value, _name);
    }

private:
    std::string _name;
    std::string _help;
    std::vector<std::string> _labels;
    std::vector<const char*> _labels_c;
};

struct Counter {
    Counter(std::string name, std::string help, std::vector<std::string> labels) :
            _name(std::move(name)), _help(std::move(help)), _labels(std::move(labels)) {
        K2LOG_D(log::k2Client, "create counter metric: {}", _name);
        _labels_c.reserve(_labels.size());
        for (auto& l : _labels) {
            _labels_c.push_back(l.c_str());
        }
    }

    ~Counter() {
        K2LOG_D(log::k2Client, "destroy counter metric: {}", _name);
    }

    void add(double value, const char** label_values=NULL) {
        K2LOG_D(log::k2Client, "add counter metric: {} for name {}", value, _name);
    }

private:
    std::string _name;
    std::string _help;
    std::vector<std::string> _labels;
    std::vector<const char*> _labels_c;
};
}
}
