#pragma once

#include <string>
#include <vector>

#include "k2_includes.h"
#include "k2_log.h"

extern "C" {
#include <prom.h>

namespace k2pg::metrics {
struct Histogram {
    Histogram(std::string name, std::string help, int start, double factor, int count, std::vector<std::string> labels):
            _name(std::move(name)), _help(std::move(help)), _start(start), _factor(factor), _count(count), _labels(std::move(labels)) {
        K2LOG_D(log::pg, "creating histogram metric: {}", _name);
        _labels_c.reserve(_labels.size());
        for (auto& l: _labels) {
            _labels_c.push_back(l.c_str());
        }
        _hist = prom_collector_registry_must_register_metric(
            prom_histogram_new(
                _name.c_str(),
                _help.c_str(),
                prom_histogram_buckets_exponential(_start, _factor, _count),
                _labels.size(),
                _labels.empty() ? NULL : _labels_c.data()));
    }

    ~Histogram() {
        K2LOG_D(log::pg, "deleting histogram metric: {}", _name);

        // do not destroy the metric as below
        // - destroying the entire registry should take care of the metrics registered in it
        // prom_histogram_destroy(_hist);
    }

    void observe(double value, const char** label_values=NULL) {
        if (!_hist) return;
        K2LOG_D(log::pg, "observe value in histogram metric: {}", _name);
        int r = prom_histogram_observe(_hist, value, label_values);
        if (r) {
            throw std::runtime_error("Unable to record history metric");
        }
    }

    void observe(k2::Duration value, const char** label_values=NULL) {
        if (!_hist) return;
        K2LOG_D(log::pg, "observe duration in histogram metric: {}", _name);
        int r = prom_histogram_observe(_hist, (double)k2::usec(value).count(), label_values);
        if (r) {
            throw std::runtime_error("Unable to record history metric");
        }
    }

private:
    std::string _name;
    std::string _help;
    int _start{0};
    double _factor{0};
    int _count{0};
    std::vector<std::string> _labels;
    std::vector<const char*> _labels_c;
    prom_histogram_t *_hist{0};
};

struct Gauge {
    Gauge(std::string name, std::string help, std::vector<std::string> labels) :
            _name(std::move(name)), _help(std::move(help)), _labels(std::move(labels)) {
        K2LOG_D(log::pg, "create gauge metric: {}", _name);
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
        K2LOG_D(log::pg, "destroy gauge metric: {}", _name);

        // do not destroy the metric as below
        // - destroying the entire registry should take care of the metrics registered in it
        // prom_gauge_destroy(_gauge);
    }

    void set(double value, const char** label_values=NULL) {
        if (!_gauge) return;
        K2LOG_D(log::pg, "set gauge metric: {}", _name);
        int r = prom_gauge_set(_gauge, value, label_values);
        if (r) {
            throw std::runtime_error("Unable to set gauge metric");
        }
    }

    void add(double value, const char** label_values=NULL) {
        K2LOG_D(log::pg, "add gauge metric: {}", _name);
        if (!_gauge) return;
        int r = value >= 0 ? prom_gauge_add(_gauge, value, label_values) : prom_gauge_sub(_gauge, value, label_values);
        if (r) {
            throw std::runtime_error("Unable to add gauge metric");
        }
    }

private:
    std::string _name;
    std::string _help;
    std::vector<std::string> _labels;
    std::vector<const char*> _labels_c;
    prom_gauge_t* _gauge{0};
};

struct Counter {
    Counter(std::string name, std::string help, std::vector<std::string> labels) :
            _name(std::move(name)), _help(std::move(help)), _labels(std::move(labels)) {
        K2LOG_D(log::pg, "create counter metric: {}", _name);
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
        K2LOG_D(log::pg, "destroy counter metric: {}", _name);
        // do not destroy the metric as below
        // - destroying the entire registry should take care of the metrics registered in it
        // prom_counter_destroy(_counter);
    }

    void add(double value, const char** label_values=NULL) {
        if (!_counter) return;
        K2LOG_D(log::pg, "add counter metric: {}", _name);
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
    prom_counter_t* _counter{0};
};
}
}
