import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class Json {
    static String encode(Object value) {
        if (value == null) return "null";
        if (value instanceof String s) {
            StringBuilder out = new StringBuilder("\"");
            for (int i = 0; i < s.length(); i++) {
                char c = s.charAt(i);
                if (c == '"' || c == '\\') out.append('\\').append(c);
                else if (c < 32 || Character.isSurrogate(c)) out.append(String.format("\\u%04x", (int)c));
                else out.append(c);
            }
            return out.append('"').toString();
        }
        if (value instanceof Boolean || value instanceof Number) return value.toString();
        List<String> parts = new ArrayList<>();
        if (value instanceof Map<?, ?> map) {
            map.forEach((k, v) -> parts.add(encode(k) + ":" + encode(v)));
            return "{" + String.join(",", parts) + "}";
        }
        if (value instanceof List<?> list) {
            list.forEach(v -> parts.add(encode(v)));
            return "[" + String.join(",", parts) + "]";
        }
        throw new IllegalArgumentException("Tipo JSON inválido");
    }

    private final String text;
    private int index;

    private Json(String text) { this.text = text; }

    static Object decode(String text) {
        Json parser = new Json(text);
        Object value = parser.value(0);
        parser.space();
        if (parser.index != text.length()) throw parser.invalid();
        return value;
    }

    private Object value(int depth) {
        if (depth > 32) throw invalid();
        space();
        if (index >= text.length()) throw invalid();
        return switch (text.charAt(index)) {
            case '"' -> string();
            case '{' -> object(depth + 1);
            case '[' -> array(depth + 1);
            case 't' -> literal("true", true);
            case 'f' -> literal("false", false);
            case 'n' -> literal("null", null);
            default -> integer();
        };
    }

    private Map<String, Object> object(int depth) {
        Map<String, Object> result = new LinkedHashMap<>();
        expect('{');
        if (take('}')) return result;
        do {
            space();
            String key = string();
            expect(':');
            if (result.containsKey(key)) throw invalid();
            result.put(key, value(depth));
        } while (take(','));
        expect('}');
        return result;
    }

    private List<Object> array(int depth) {
        List<Object> result = new ArrayList<>();
        expect('[');
        if (take(']')) return result;
        do { result.add(value(depth)); } while (take(','));
        expect(']');
        return result;
    }

    private Object literal(String token, Object value) {
        if (!text.startsWith(token, index)) throw invalid();
        index += token.length();
        return value;
    }

    private long integer() {
        int start = index;
        if (index < text.length() && text.charAt(index) == '-') index++;
        while (index < text.length() && Character.isDigit(text.charAt(index))) index++;
        return Long.parseLong(text.substring(start, index));
    }

    private String string() {
        expect('"');
        StringBuilder out = new StringBuilder();
        while (index < text.length()) {
            char c = text.charAt(index++);
            if (c == '"') return out.toString();
            if (c < 32) throw invalid();
            if (c != '\\') { out.append(c); continue; }
            if (index >= text.length()) throw invalid();
            switch (text.charAt(index++)) {
                case '"', '\\', '/' -> out.append(text.charAt(index - 1));
                case 'b' -> out.append('\b');
                case 'f' -> out.append('\f');
                case 'n' -> out.append('\n');
                case 'r' -> out.append('\r');
                case 't' -> out.append('\t');
                case 'u' -> {
                    if (text.length() - index < 4) throw invalid();
                    out.append((char)Integer.parseInt(text.substring(index, index + 4), 16));
                    index += 4;
                }
                default -> throw invalid();
            }
        }
        throw invalid();
    }

    private void space() {
        while (index < text.length() && " \r\n\t".indexOf(text.charAt(index)) >= 0) index++;
    }

    private boolean take(char expected) {
        space();
        if (index >= text.length() || text.charAt(index) != expected) return false;
        index++;
        return true;
    }

    private void expect(char expected) {
        if (!take(expected)) throw invalid();
    }

    private IllegalArgumentException invalid() { return new IllegalArgumentException("JSON inválido"); }

    @SuppressWarnings("unchecked")
    static Map<String, Object> map(Object value) { return (Map<String, Object>) value; }

    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> rows(Object value) { return (List<Map<String, Object>>) value; }
}
