#pragma once

#include <string>
#include <vector>
#include <k2/common/Chrono.h>

extern "C" {
#include <prom.h>

namespace k2pg::metrics {
struct Histogram {
    Histogram(std::string name, std::string help, int start, double factor, int count, std::vector<std::string> labels):
            _name(std::move(name)), _help(std::move(help)), _labels(std::move(labels)) {
        _labels_c.reserve(_labels.size());
        for (auto& l: _labels) {
            _labels_c.push_back(l.c_str());
        }
        _hist = prom_collector_registry_must_register_metric(
            prom_histogram_new(
                _name.c_str(),
                _help.c_str(),
                prom_histogram_buckets_exponential(start, factor, count),
                _labels.size(),
                _labels.empty() ? NULL : _labels_c.data()));
    }
    ~Histogram() {
        prom_histogram_destroy(_hist);
    }

    void observe(double value, const char** label_values=NULL) {
        int r = prom_histogram_observe(_hist, value, label_values);
        if (r) {
            throw std::runtime_error("Unable to record history metric");
        }
    }

    void observe(k2::Duration value, const char** label_values=NULL) {
        int r = prom_histogram_observe(_hist, (double) k2::usec(value).count(), label_values);
        if (r) {
            throw std::runtime_error("Unable to record history metric");
        }
    }

private:
    std::string _name;
    std::string _help;
    std::vector<std::string> _labels;
    std::vector<const char*> _labels_c;
    prom_histogram_t *_hist;
};

struct Gauge {
    Gauge(std::string name, std::string help, std::vector<std::string> labels) :
            _name(std::move(name)), _help(std::move(help)), _labels(std::move(labels)) {
        _labels_c.reserve(_labels.size());
        for (auto& l : _labels) {
            _labels_c.push_back(l.c_str());
        }
        _gauge = prom_collector_registry_must_register_metric(
            prom_gauge_new(
                _name.c_str(),
                _help.c_str(),
                _labels.size(),
                _labels.empty() ? NULL : _labels_c.data()));
    }
    ~Gauge() {
        prom_gauge_destroy(_gauge);
    }

    void set(double value, const char** label_values=NULL) {
        int r = prom_gauge_set(_gauge, value, label_values);
        if (r) {
            throw std::runtime_error("Unable to set gauge metric");
        }
    }

    void add(double value, const char** label_values=NULL) {
        int r = prom_gauge_add(_gauge, value, label_values);
        if (r) {
            throw std::runtime_error("Unable to add gauge metric");
        }
    }

private:
    std::string _name;
    std::string _help;
    std::vector<std::string> _labels;
    std::vector<const char*> _labels_c;
    prom_gauge_t* _gauge;
};

struct Counter {
    Counter(std::string name, std::string help, std::vector<std::string> labels) :
            _name(std::move(name)), _help(std::move(help)), _labels(std::move(labels)) {
        _labels_c.reserve(_labels.size());
        for (auto& l : _labels) {
            _labels_c.push_back(l.c_str());
        }
        _counter = prom_collector_registry_must_register_metric(
            prom_counter_new(
                _name.c_str(),
                _help.c_str(),
                _labels.size(),
                _labels.empty() ? NULL : _labels_c.data()));
    }

    ~Counter() {
        prom_counter_destroy(_counter);
    }

    void add(double value, const char** label_values=NULL) {
        int r = prom_counter_add(_counter, value, label_values);
        if (r) {
            throw std::runtime_error("Unable to add counter metric");
        }
    }

private:
    std::string _name;
    std::string _help;
    std::vector<std::string> _labels;
    std::vector<const char*> _labels_c;
    prom_counter_t* _counter;
};
}
}
