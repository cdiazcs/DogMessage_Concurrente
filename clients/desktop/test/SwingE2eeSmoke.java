import java.awt.Component;
import java.awt.Container;
import java.awt.Window;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.regex.Pattern;
import javax.swing.*;

public final class SwingE2eeSmoke {
    private static JFrame frame;

    private static <T> T gui(Callable<T> action) throws Exception {
        var task = new FutureTask<>(action);
        SwingUtilities.invokeAndWait(task);
        return task.get();
    }

    private static List<Component> tree(Component root) {
        List<Component> result = new ArrayList<>();
        result.add(root);
        if (root instanceof Container parent) {
            for (Component child : parent.getComponents()) result.addAll(tree(child));
        }
        return result;
    }

    private static <T extends Component> List<T> of(Component root, Class<T> type) {
        return tree(root).stream().filter(type::isInstance).map(type::cast).toList();
    }

    private static void waitFor(String label, Callable<Boolean> condition) throws Exception {
        long end = System.nanoTime() + 35_000_000_000L;
        while (System.nanoTime() < end) {
            if (gui(condition)) return;
            Thread.sleep(60);
        }
        throw new IllegalStateException("Estado Swing pendiente: " + label);
    }

    private static void input(String label, String value) {
        for (var item : of(frame, JLabel.class)) {
            if (label.equals(item.getText()) && item.getLabelFor() instanceof JTextField input) {
                input.setText(value);
                return;
            }
        }
        throw new IllegalStateException("Campo ausente");
    }

    private static JButton button(String text) {
        return of(frame, JButton.class).stream().filter(item -> text.equals(item.getText())).findFirst().orElseThrow();
    }

    private static JDialog dialog() {
        for (var window : Window.getWindows()) {
            if (window instanceof JDialog d && d.isShowing() && d.getOwner() == frame) return d;
        }
        return null;
    }

    private static JOptionPane option() {
        return of(dialog(), JOptionPane.class).get(0);
    }

    private static boolean label(String text) {
        return of(frame, JLabel.class).stream().anyMatch(item -> text.equals(item.getText()));
    }

    private static boolean showingText(String text) {
        return of(frame, JTextArea.class).stream().anyMatch(item -> item.getText().contains(text));
    }

    private static void choose(String action, String file) throws Exception {
        SwingUtilities.invokeLater(() -> button(action).doClick(0));
        waitFor(action, () -> dialog() != null && !of(dialog(), JFileChooser.class).isEmpty());
        gui(() -> {
            var chooser = of(dialog(), JFileChooser.class).get(0);
            chooser.setSelectedFile(new java.io.File(file));
            chooser.approveSelection();
            return null;
        });
    }

    private static void run(String[] args) throws Exception {
        AppMain.main(new String[0]);
        waitFor("ventana", () -> {
            for (var window : Window.getWindows()) {
                if (window instanceof JFrame candidate && candidate.isShowing()) { frame = candidate; return true; }
            }
            return false;
        });
        gui(() -> {
            input("Servidor TLS", "127.0.0.1");
            input("Puerto", args[0]);
            input("Usuario", "swing_secure");
            input("Nombre para registro", "Swing protegido");
            input("Contraseña", "solo-prueba-swing-segura-2026");
            button("Registrar").doClick(0);
            return null;
        });
        waitFor("registro", () -> label("Confirmado"));
        gui(() -> { input("Contraseña", "solo-prueba-swing-segura-2026"); button("Iniciar sesión").doClick(0); return null; });
        waitFor("sesión", () -> of(frame, JList.class).size() >= 2 && of(frame, JList.class).get(0).isEnabled());
        SwingUtilities.invokeLater(() -> button("Identidad de dispositivo").doClick(0));
        waitFor("clave local", () -> dialog() != null);
        gui(() -> {
            of(dialog(), JPasswordField.class).get(0).setText("clave-local-sintetica-2026");
            option().setValue(JOptionPane.OK_OPTION);
            return null;
        });
        waitFor("huella pública", () -> dialog() != null && String.valueOf(option().getMessage()).contains("Huella SHA-256"));
        String mark = gui(() -> {
            var match = Pattern.compile("[a-f0-9]{64}").matcher(String.valueOf(option().getMessage()));
            if (!match.find()) throw new AssertionError("Sin huella");
            option().setValue(JOptionPane.OK_OPTION);
            return match.group();
        });
        System.out.println("PUBLIC_SWING " + mark);
        Path expected = Path.of(args[1]);
        waitFor("conversación del navegador", () -> Files.exists(expected)
                && of(frame, JList.class).get(0).isEnabled() && of(frame, JList.class).get(0).getModel().getSize() > 0);
        gui(() -> { of(frame, JList.class).get(0).setSelectedIndex(0); return null; });
        waitFor("historial cifrado", () -> button("Enviar").isEnabled() && showingText("Huella del emisor sin verificar"));
        SwingUtilities.invokeLater(() -> button("Configurar cifrado").doClick(0));
        waitFor("comparar huella", () -> dialog() != null);
        String browser = Files.readString(expected).strip();
        gui(() -> {
            if (!String.valueOf(option().getMessage()).contains(browser)) throw new AssertionError("Huella alterada");
            option().setValue(JOptionPane.YES_OPTION);
            return null;
        });
        waitFor("descifrado", () -> button("Enviar").isEnabled() && showingText("Texto E2EE desde Chrome"));
        gui(() -> {
            var fields = of(frame, JTextField.class);
            fields.get(fields.size() - 1).setText("Texto E2EE desde Swing 🐕");
            button("Enviar").doClick(0);
            return null;
        });
        waitFor("mensaje confirmado", () -> showingText("Texto E2EE desde Swing"));
        System.out.println("PASS Swing cifró y descifró texto E2EE");
        if (args.length == 4) {
            choose("Adjuntar", args[2]);
            waitFor("PDF cifrado confirmado", () -> label("Archivo confirmado"));
            waitFor("PNG cifrado recibido", () -> of(frame, JLabel.class).stream()
                .anyMatch(item -> item.getText() != null && item.getText().startsWith("pixel.png ·")));
            gui(() -> {
                for (var item : of(frame, JLabel.class)) {
                    if (item.getText() != null && item.getText().startsWith("pixel.png ·")) {
                        SwingUtilities.invokeLater(() -> of(item.getParent(), JButton.class).stream()
                            .filter(candidate -> "Descargar".equals(candidate.getText())).findFirst().orElseThrow().doClick(0));
                    }
                }
                return null;
            });
            waitFor("destino PNG", () -> dialog() != null && !of(dialog(), JFileChooser.class).isEmpty());
            gui(() -> {
                var chooser = of(dialog(), JFileChooser.class).get(0);
                chooser.setSelectedFile(new java.io.File(args[3]));
                chooser.approveSelection();
                return null;
            });
            waitFor("PNG descifrado", () -> label("Integridad verificada") && Files.isRegularFile(Path.of(args[3])));
            System.out.println("PASS Swing adjuntó PDF cifrado y descargó el PNG de Chrome");
        }
    }

    public static void main(String[] args) {
        try { run(args); System.exit(0); }
        catch (Throwable error) { error.printStackTrace(); System.exit(1); }
    }
}
