package io.github.jonesbusy;

import java.util.List;

/** OCI Distribution Spec error response envelope. */
public record OciError(List<ErrorEntry> errors) {

    public record ErrorEntry(String code, String message, String detail) {}

    public static OciError of(String code, String message) {
        return new OciError(List.of(new ErrorEntry(code, message, null)));
    }
}
