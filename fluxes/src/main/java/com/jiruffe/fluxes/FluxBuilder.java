package com.jiruffe.fluxes;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * influxDB 查询语句构造
 *
 * @author Jiruffe
 */
public class FluxBuilder {

    private boolean prettified = false;
    private final Collection<String> imports = new LinkedList<>();
    private final Collection<String> operations = new LinkedList<>();

    public Flux build() {
        return new Flux(prettified, imports, operations);
    }

    public FluxBuilder prettified() {
        prettified = true;
        return this;
    }

    public FluxBuilder unprettified() {
        prettified = false;
        return this;
    }

    public FluxBuilder imports(String... importation) {
        imports.addAll(Arrays.asList(importation));
        return this;
    }

    public FluxBuilder then(String operation) {
        operations.add(operation);
        return this;
    }

    public FluxBuilder from(String bucket) {
        return then("from(bucket: \"" + bucket + "\")");
    }

    public FluxBuilder range(String start) {
        return then("range(start: " + start + ")");
    }

    public FluxBuilder range(String start, String stop) {
        return then("range(start: " + start + ", stop: " + stop + ")");
    }

    public FluxBuilder filter(String fn) {
        return then("filter(fn: " + fn + ")");
    }

    public FluxBuilder measurement(String measurement) {
        return filter("(r) => r[\"_measurement\"] == \"" + measurement + "\"");
    }

    public FluxBuilder group() {
        return then("group()");
    }

    public FluxBuilder group(String... columns) {
        return then("group(columns: [" +
                Stream.of(columns)
                        .map(s -> "\"" + s + "\"")
                        .collect(Collectors.joining(", "))
                + "])");
    }

    public FluxBuilder pivot(Collection<String> rowKey, Collection<String> columnKey, String valueColumn) {
        return then("pivot(rowKey: [" +
                rowKey.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(", "))
                + "], columnKey: [" +
                columnKey.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(", "))
                + "], valueColumn: \"" + valueColumn + "\")");
    }

    public FluxBuilder sort() {
        return then("sort()");
    }

    public FluxBuilder sort(boolean desc) {
        return then("sort(desc: " + desc + ")");
    }

    public FluxBuilder sort(String... columns) {
        return then("sort(columns: [" +
                Stream.of(columns)
                        .map(s -> "\"" + s + "\"")
                        .collect(Collectors.joining(", "))
                + "])");
    }

    public FluxBuilder sort(boolean desc, String... columns) {
        return then("sort(columns: [" +
                Stream.of(columns)
                        .map(s -> "\"" + s + "\"")
                        .collect(Collectors.joining(", "))
                + "], desc: " + desc + ")");
    }

    public FluxBuilder count() {
        return then("count()");
    }

    public FluxBuilder count(String column) {
        return then("count(column: \"" + column + "\")");
    }

    public FluxBuilder limit(int n) {
        return then("limit(n: " + n + ")");
    }

    public FluxBuilder limit(int n, int offset) {
        return then("limit(n: " + n + ", offset: " + offset + ")");
    }

    public FluxBuilder yield() {
        return then("yield()");
    }

    public FluxBuilder yield(String name) {
        return then("yield(name: \"" + name + "\")");
    }

}
