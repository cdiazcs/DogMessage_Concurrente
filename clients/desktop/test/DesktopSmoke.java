import java.awt.Component;
import java.awt.Container;
import java.awt.Window;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import javax.swing.*;

public final class DesktopSmoke {
    private static JFrame frame;

    private static <T> T gui(Callable<T> action) throws Exception {
        FutureTask<T> task = new FutureTask<>(action);
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

    private static void waitFor(Callable<Boolean> condition) throws Exception {
        long end = System.nanoTime() + 15_000_000_000L;
        while (System.nanoTime() < end) {
            if (gui(condition)) return;
            Thread.sleep(50);
        }
        throw new IllegalStateException("Estado Swing no observado");
    }

    private static List<Component> widgets() { return tree(frame); }

    private static <T extends Component> List<T> of(Class<T> type) {
        return widgets().stream().filter(type::isInstance).map(type::cast).toList();
    }

    private static void field(String label, String value) {
        for (JLabel item : of(JLabel.class)) {
            if (label.equals(item.getText()) && item.getLabelFor() instanceof JTextField input) {
                input.setText(value);
                return;
            }
        }
        throw new IllegalStateException("Campo ausente");
    }

    private static void click(String text) {
        of(JButton.class).stream().filter(button -> text.equals(button.getText())).findFirst()
            .orElseThrow().doClick(0);
    }

    private static boolean label(String text) {
        return of(JLabel.class).stream().anyMatch(item -> text.equals(item.getText()));
    }

    public static void main(String[] args) {
        int status = 0;
        try {
            scenario(args);
        } catch (Exception error) {
            error.printStackTrace();
            status = 1;
        }
        System.exit(status);
    }

    private static void choose(String action, String path) throws Exception {
        SwingUtilities.invokeLater(() -> click(action));
        waitFor(() -> java.util.Arrays.stream(Window.getWindows()).anyMatch(window ->
            window.isShowing() && tree(window).stream().anyMatch(JFileChooser.class::isInstance)));
        gui(() -> {
            var chooser = java.util.Arrays.stream(Window.getWindows()).filter(Window::isShowing)
                .flatMap(window -> tree(window).stream()).filter(JFileChooser.class::isInstance)
                .map(JFileChooser.class::cast).findFirst().orElseThrow();
            chooser.setSelectedFile(new java.io.File(path));
            chooser.approveSelection();
            return null;
        });
    }

    private static void scenario(String[] args) throws Exception {
        AppMain.main(new String[0]);
        try {
            waitFor(() -> {
                for (Window window : Window.getWindows()) {
                    if (window instanceof JFrame candidate && "Dog Messenger".equals(candidate.getTitle()) && candidate.isShowing()) {
                        frame = candidate;
                        return true;
                    }
                }
                return false;
            });
            boolean receive = args.length > 1 && args[1].equals("receive");
            gui(() -> {
                field("Servidor TLS", "127.0.0.1");
                field("Puerto", args[0]);
                field("Usuario", "swing_test");
                field("Nombre para registro", "Cliente Swing");
                field("Contraseña", "solo-prueba-swing-2026");
                if (!receive) click("Registrar");
                return null;
            });
            if (!receive) waitFor(() -> label("Confirmado"));
            gui(() -> {
                field("Contraseña", "solo-prueba-swing-2026");
                click("Iniciar sesión");
                return null;
            });
            waitFor(() -> of(JList.class).stream().anyMatch(list -> list.getModel().getSize() > 0));
            gui(() -> {
                for (JList<?> list : of(JList.class)) {
                    if (list.getModel().getSize() == 0) continue;
                    for (int index = 0; index < list.getModel().getSize(); index++) {
                        String item = list.getModel().getElementAt(index).toString();
                        if (receive ? item.contains("Cliente web · web_test") :
                            item.contains("Persona 0 · client0") || item.contains("Usuario 0 · files0")) {
                            list.setSelectedIndex(index);
                            break;
                        }
                    }
                }
                click("Abrir chat");
                return null;
            });
            waitFor(() -> of(JButton.class).stream().anyMatch(button -> "Enviar".equals(button.getText()) && button.isEnabled()));
            String message = receive ? "Respuesta de Swing 🐕" : "Desde Swing 🐕";
            if (receive) {
                waitFor(() -> of(JTextArea.class).stream()
                    .anyMatch(area -> area.getText().contains("<script>throw 123</script>")));
            }
            gui(() -> {
                var fields = of(JTextField.class);
                fields.get(fields.size() - 1).setText(message);
                click("Enviar");
                return null;
            });
            waitFor(() -> of(JTextArea.class).stream().anyMatch(area -> area.getText().contains(message)));
            if (receive) {
                System.out.println("PASS Swing leyó el mensaje web y respondió en la misma conversación");
                if (args.length > 3) {
                    waitFor(() -> of(JLabel.class).stream().anyMatch(item -> item.getText() != null
                        && item.getText().startsWith("pixel.png ·")));
                    choose("Descargar", args[3]);
                    waitFor(() -> label("Integridad verificada"));
                    choose("Adjuntar", args[2]);
                    waitFor(() -> label("Archivo confirmado"));
                    System.out.println("PASS Swing descargó el PNG web y adjuntó el PDF");
                }
                return;
            }
            SwingUtilities.invokeLater(() -> click("Crear grupo"));
            waitFor(() -> java.util.Arrays.stream(Window.getWindows())
                .anyMatch(window -> window instanceof JDialog && window.isShowing()));
            gui(() -> {
                JDialog dialog = (JDialog)java.util.Arrays.stream(Window.getWindows())
                    .filter(window -> window instanceof JDialog && window.isShowing()).findFirst().orElseThrow();
                for (Component component : tree(dialog)) {
                    if (component instanceof JTextField field) field.setText("Grupo Swing");
                }
                JOptionPane option = tree(dialog).stream().filter(JOptionPane.class::isInstance)
                    .map(JOptionPane.class::cast).findFirst().orElseThrow();
                option.setInputValue("Grupo Swing");
                option.setValue(JOptionPane.OK_OPTION);
                return null;
            });
            waitFor(() -> label("Grupo Swing"));
            System.out.println("PASS Swing visible, registro, login, conversación, envío, historial y grupo por TCP TLS");
        } finally {
            gui(() -> {
                if (frame != null) frame.dispose();
                return null;
            });
        }
    }
}
