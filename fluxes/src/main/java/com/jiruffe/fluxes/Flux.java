package com.jiruffe.fluxes;

import java.io.Serializable;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * influxDB 查询语句构造
 *
 * @author Jiruffe
 */
public class Flux implements Serializable {

    private static final long serialVersionUID = -6934617183308764065L;

    private final boolean prettified;
    private final Collection<String> imports;
    private final Collection<String> operations;

    public Flux(boolean prettified, Collection<String> imports, Collection<String> operations) {
        if (Objects.isNull(imports)) {
            throw new IllegalArgumentException(new NullPointerException("imports"));
        }
        if (Objects.isNull(operations)) {
            throw new IllegalArgumentException(new NullPointerException("stream"));
        }
        this.prettified = prettified;
        this.imports = imports;
        this.operations = operations;
    }

    public static FluxBuilder builder() {
        return new FluxBuilder();
    }

    @Override
    public String toString() {
        return (imports.isEmpty() ? "" : (imports.stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(((Predicate<String>) String::isEmpty).negate())
                .map(s -> "import \"" + s + "\"")
                .collect(Collectors.joining("\n")) + (prettified ? "\n\n" : "\n")))
                +
                operations.stream()
                        .filter(Objects::nonNull)
                        .map(String::trim)
                        .filter(((Predicate<String>) String::isEmpty).negate())
                        .collect(Collectors.joining(prettified ? "\n  |> " : "|>"));
    }

}
