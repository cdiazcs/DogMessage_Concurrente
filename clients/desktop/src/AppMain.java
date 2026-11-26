import java.awt.BorderLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import javax.swing.*;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

public final class AppMain {
    private final JFrame frame = new JFrame("Dog Messenger");
    private final JLabel connection = new JLabel("Sin sesión");
    private final JLabel feedback = new JLabel(" ");
    private final JLabel title = new JLabel("Elige una conversación");
    private final JTextField host = new JTextField("localhost"), port = new JTextField("9443");
    private final JTextField username = new JTextField(), display = new JTextField(), search = new JTextField();
    private final JPasswordField password = new JPasswordField();
    private final JTextField input = new JTextField();
    private final DefaultListModel<Row> conversationRows = new DefaultListModel<>(), userRows = new DefaultListModel<>();
    private final JList<Row> conversations = new JList<>(conversationRows), users = new JList<>(userRows);
    private final JPanel messages = new JPanel();
    private final JButton more = new JButton("Siguiente página"), retry = new JButton("Reintentar envío");
    private final JButton identityButton = new JButton("Identidad de dispositivo");
    private final JButton linkButton = new JButton("Vincular dispositivo");
    private final JButton shareButton = new JButton("Compartir claves");
    private final JButton revokeButton = new JButton("Revocar dispositivo");
    private final JButton secureButton = new JButton("Configurar cifrado");
    private E2ee.Device localDevice;
    private final Set<String> encrypted = new HashSet<>();

    private final JButton attach = new JButton("Adjuntar"), resumeFile = new JButton("Reanudar archivo");
    private final JButton cancelFile = new JButton("Cancelar archivo");
    private final JProgressBar fileProgress = new JProgressBar(0, 100);
    private final JLabel fileStatus = new JLabel(" ");
    private MediaTransfer.Upload upload;
    private boolean mediaBusy;

    private final JButton send = new JButton("Enviar"), members = new JButton("Integrantes");
    private final JButton add = new JButton("Añadir"), remove = new JButton("Retirar");
    private final Set<String> seen = new HashSet<>();
    private final Timer poll = new Timer(3_000, event -> poll());
    private List<Map<String, Object>> directory = List.of(), available = List.of();
    private NetworkClient network;
    private NetworkClient.Request pending, pendingAuth;
    private String session, own, current, cursor = "0", minimum = "0";
    private boolean busy, hasMore, updating;
    private int ticks;

    private record Row(String id, String label) {
        @Override public String toString() { return label; }
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> new AppMain().start());
    }

    private void start() {
        title.putClientProperty("html.disable", Boolean.TRUE);
        DefaultListCellRenderer plain = new DefaultListCellRenderer();
        plain.putClientProperty("html.disable", Boolean.TRUE);
        conversations.setCellRenderer(plain);
        users.setCellRenderer(plain);
        for (JList<Row> list : List.of(conversations, users)) {
            list.setCellRenderer(new DefaultListCellRenderer() {
                @Override
                public java.awt.Component getListCellRendererComponent(JList<?> values, Object row,
                        int index, boolean selected, boolean focus) {
                    putClientProperty("html.disable", Boolean.TRUE);
                    return super.getListCellRendererComponent(values, row, index, selected, focus);
                }
            });
        }
        title.putClientProperty("html.disable", Boolean.TRUE);
        feedback.putClientProperty("html.disable", Boolean.TRUE);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(960, 640);
        frame.setMinimumSize(new Dimension(640, 450));
        frame.setLocationRelativeTo(null);
        showLogin();
        frame.setVisible(true);
    }

    private void layout(JPanel content) {
        JPanel root = new JPanel(new BorderLayout(10, 10));
        root.setBorder(BorderFactory.createEmptyBorder(12, 12, 12, 12));
        root.setBackground(new Color(0xf8fafc));
        JPanel banner = new JPanel(new BorderLayout());
        banner.setBackground(new Color(0x164e63));
        banner.setBorder(BorderFactory.createEmptyBorder(10, 12, 10, 12));
        JLabel brand = new JLabel("Dog Messenger");
        brand.setForeground(Color.WHITE);
        connection.setForeground(Color.WHITE);
        banner.add(brand, BorderLayout.WEST);
        banner.add(connection, BorderLayout.EAST);
        root.add(banner, BorderLayout.NORTH);
        root.add(content, BorderLayout.CENTER);
        root.add(feedback, BorderLayout.SOUTH);
        frame.setContentPane(root);
        frame.revalidate();
        frame.repaint();
    }

    private JPanel actions(JComponent... components) {
        JPanel row = new JPanel(new FlowLayout(FlowLayout.LEADING));
        for (JComponent component : components) row.add(component);
        return row;
    }

    private void showLogin() {
        poll.stop();
        session = own = current = null;
        pending = pendingAuth = null;
        localDevice = null;
        encrypted.clear();
        if (upload != null && !upload.completing) upload.cancel.set(true);
        upload = null;
        fileStatus.setText(" ");
        fileProgress.setVisible(false);
        input.setText("");
        password.setText("");
        cursor = minimum = "0";
        clearThread();
        connection.setText("Sin sesión");
        JPanel form = new JPanel(new GridLayout(0, 2, 8, 10));
        addField(form, "Servidor TLS", host);
        addField(form, "Puerto", port);
        addField(form, "Usuario", username);
        addField(form, "Nombre para registro", display);
        addField(form, "Contraseña", password);
        JButton login = new JButton("Iniciar sesión"), register = new JButton("Registrar");
        login.addActionListener(event -> authenticate("login"));
        register.addActionListener(event -> authenticate("register"));
        form.add(login);
        form.add(register);
        JPanel pane = new JPanel(new BorderLayout());
        pane.add(form, BorderLayout.NORTH);
        layout(pane);
        frame.getRootPane().setDefaultButton(login);
    }

    private void addField(JPanel panel, String text, JTextField field) {
        JLabel label = new JLabel(text);
        label.setLabelFor(field);
        panel.add(label);
        panel.add(field);
    }

    private void authenticate(String op) {
        if (busy) return;
        char[] value = password.getPassword();
        String secret = new String(value);
        Arrays.fill(value, '\0');
        Map<String, Object> data = op.equals("login")
            ? Map.of("username", username.getText().strip(), "password", secret)
            : Map.of("username", username.getText().strip(), "password", secret, "display", display.getText().strip());
        try {
            String endpoint = host.getText().strip() + ":" + port.getText().strip();
            if (!endpoint.equals(connection.getClientProperty("endpoint"))) pendingAuth = null;
            network = new NetworkClient(host.getText().strip(), Integer.parseInt(port.getText().strip()));
            connection.putClientProperty("endpoint", endpoint);
        } catch (IllegalArgumentException error) {
            feedback.setText("Dirección o puerto inválido");
            return;
        }
        if (pendingAuth == null || !pendingAuth.op().equals(op) || !pendingAuth.data().equals(data)) {
            pendingAuth = new NetworkClient.Request(op, data);
        }
        NetworkClient.Request attempt = pendingAuth;
        work(() -> call(attempt), valueResult -> {
            var result = Json.map(valueResult);
            boolean unfinished = report(result);
            if (!unfinished) {
                password.setText("");
                pendingAuth = null;
            }
            if (result.get("session") instanceof String token) {
                session = token;
                showChat();
                refresh(true);
            }
        });
    }

    private void showChat() {
        JPanel sidebar = new JPanel(new GridLayout(2, 1, 6, 8));
        JPanel upper = new JPanel(new BorderLayout());
        JButton logout = new JButton("Salir");
        logout.addActionListener(event -> mutate("logout", Map.of(), result -> {
            if (applied(result)) showLogin();
        }));
        JLabel identity = new JLabel();
        identity.putClientProperty("html.disable", Boolean.TRUE);
        identity.setText(username.getText());
        JPanel credentials = new JPanel(new GridLayout(0, 1));
        credentials.add(actions(identity, logout));
        credentials.add(actions(identityButton));
        credentials.add(actions(linkButton, revokeButton));
        upper.add(credentials, BorderLayout.NORTH);
        upper.add(new JScrollPane(conversations));
        JPanel lower = new JPanel(new BorderLayout());
        JLabel searchLabel = new JLabel("Buscar usuarios");
        searchLabel.setLabelFor(search);
        JPanel searchBox = new JPanel(new BorderLayout(8, 0));
        searchBox.add(searchLabel, BorderLayout.WEST);
        searchBox.add(search);
        lower.add(searchBox, BorderLayout.NORTH);
        lower.add(new JScrollPane(users));
        JButton direct = new JButton("Abrir chat"), group = new JButton("Crear grupo");
        direct.addActionListener(event -> {
            Row peer = users.getSelectedValue();
            if (peer != null) change("direct", Map.of("peer", peer.id()));
        });
        group.addActionListener(event -> {
            String name = JOptionPane.showInputDialog(frame, "Nombre del grupo");
            if (name != null) change("group-create", Map.of("name", name,
                "members", users.getSelectedValuesList().stream().map(Row::id).toList()));
        });
        lower.add(actions(direct, group), BorderLayout.SOUTH);
        sidebar.add(upper);
        sidebar.add(lower);

        messages.setLayout(new BoxLayout(messages, BoxLayout.Y_AXIS));
        JPanel thread = new JPanel(new BorderLayout(6, 8));
        thread.add(actions(title, more, members, add, remove, secureButton, shareButton), BorderLayout.NORTH);
        thread.add(new JScrollPane(messages), BorderLayout.CENTER);
        JPanel composer = new JPanel(new BorderLayout(6, 0));
        composer.add(input, BorderLayout.CENTER);
        composer.add(actions(send, retry), BorderLayout.EAST);
        JPanel footer = new JPanel(new GridLayout(0, 1));
        footer.add(composer);
        footer.add(actions(attach, resumeFile, cancelFile, fileProgress, fileStatus));
        fileStatus.putClientProperty("html.disable", Boolean.TRUE);
        fileProgress.setStringPainted(true);
        thread.add(footer, BorderLayout.SOUTH);
        JSplitPane panes = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, sidebar, thread);
        panes.setDividerLocation(310);
        JPanel body = new JPanel(new BorderLayout());
        body.add(panes);
        layout(body);
        frame.getRootPane().setDefaultButton(send);
        installActions();
        clearThread();
        mediaControls();
        retry.setVisible(false);
        poll.start();
    }

    private boolean actionsInstalled;

    private void installActions() {
        if (actionsInstalled) return;
        actionsInstalled = true;
        conversations.addListSelectionListener(event -> {
            if (!event.getValueIsAdjusting() && !updating && conversations.getSelectedValue() != null) {
                open(conversations.getSelectedValue().id());
            }
        });
        search.getDocument().addDocumentListener(new DocumentListener() {
            public void insertUpdate(DocumentEvent event) { renderUsers(); }
            public void removeUpdate(DocumentEvent event) { renderUsers(); }
            public void changedUpdate(DocumentEvent event) { renderUsers(); }
        });
        more.addActionListener(event -> page("history"));
        members.addActionListener(event -> {
            if (current == null) return;
            String id = current;
            work(() -> call(new NetworkClient.Request("group-members", Map.of("conversation", id))), result ->
                JOptionPane.showMessageDialog(frame, ((List<?>) result).stream()
                    .map(member -> label((String)member)).toList().toString(), "Integrantes", JOptionPane.PLAIN_MESSAGE));
        });
        add.addActionListener(event -> membership("group-add"));
        remove.addActionListener(event -> membership("group-remove"));
        identityButton.addActionListener(event -> identityDialog());
        secureButton.addActionListener(event -> configureSecure());
        linkButton.addActionListener(event -> linkDialog());
        shareButton.addActionListener(event -> shareHistory());
        revokeButton.addActionListener(event -> revokeDevice());
        send.addActionListener(event -> {
            if (busy || current == null || input.getText().isBlank()) return;
            if (pending != null) {
                feedback.setText("Reintenta el envío anterior");
                return;
            }
            prepareMessage(current, input.getText());
        });
        retry.addActionListener(event -> submitMessage());
        attach.addActionListener(event -> {
            if (current == null || mediaBusy || upload != null) return;
            JFileChooser chooser = new JFileChooser();
            if (chooser.showOpenDialog(frame) == JFileChooser.APPROVE_OPTION) {
                upload = new MediaTransfer.Upload(chooser.getSelectedFile().toPath(), current);
                uploadFile();
            }
        });
        resumeFile.addActionListener(event -> uploadFile());
        cancelFile.addActionListener(event -> {
            if (upload == null || upload.completing) return;
            upload.cancel.set(true);
            if (!mediaBusy) uploadFile();
        });
    }

    private void membership(String operation) {
        Row row = users.getSelectedValue();
        if (row != null && current != null) change(operation, Map.of("conversation", current, "member", row.id()));
    }

    private void submitMessage() {
        if (busy || pending == null) return;
        var attempt = pending;
        retry.setVisible(true);
        feedback.setText("Enviando…");
        work(() -> call(attempt), value -> {
            var result = Json.map(value);
            if (!report(result)) { pending = null; retry.setVisible(false); }
            if (applied(result)) {
                input.setText("");
                if (!hasMore) page("sync");
            }
        });
    }

    private void change(String op, Map<String, Object> data) {
        mutate(op, data, result -> {
            if (applied(result)) {
                Object conversation = Json.map(result.get("application")).get("conversation");
                refresh(true, () -> { if (conversation instanceof String id) open(id); });
            }
        });
    }

    private void mutate(String op, Map<String, Object> data, Consumer<Map<String, Object>> next) {
        var request = new NetworkClient.Request(op, data);
        work(() -> call(request), value -> {
            var result = Json.map(value);
            report(result);
            next.accept(result);
        });
    }

    private void refresh(boolean usersToo) { refresh(usersToo, () -> {}); }

    private void refresh(boolean usersToo, Runnable next) {
        work(() -> {
            Object people = usersToo ? call(new NetworkClient.Request("users", Map.of())) : List.of();
            return List.of(people, call(new NetworkClient.Request("conversations", Map.of())));
        }, result -> {
            var pair = (List<?>) result;
            var payload = Json.map(pair.get(1));
            own = (String)payload.get("user");
            available = Json.rows(payload.get("items"));
            if (usersToo) {
                directory = Json.rows(pair.get(0));
                renderUsers();
            }
            updating = true;
            conversationRows.clear();
            boolean retained = false;
            for (var row : available) {
                String id = (String)row.get("id");
                if (Boolean.TRUE.equals(row.get("e2ee"))) encrypted.add(id);
                conversationRows.addElement(new Row(id, conversationLabel(row)));
                if (id.equals(current)) {
                    conversations.setSelectedIndex(conversationRows.size() - 1);
                    retained = true;
                }
            }
            updating = false;
            if (!retained) clearThread();
            next.run();
        });
    }

    private void renderUsers() {
        String term = search.getText().toLowerCase(java.util.Locale.ROOT);
        userRows.clear();
        for (var row : directory) {
            String id = (String)row.get("id");
            String text = row.get("display") + " · " + row.get("username");
            if (!id.equals(own) && text.toLowerCase(java.util.Locale.ROOT).contains(term)) {
                userRows.addElement(new Row(id, text));
            }
        }
    }

    private String label(String id) {
        return directory.stream().filter(row -> id.equals(row.get("id"))).findFirst()
            .map(row -> (String)row.get("display")).orElse(id);
    }

    private String conversationLabel(Map<String, Object> row) {
        if (row.get("name") instanceof String name) return name;
        return ((List<?>)row.get("members")).stream().filter(id -> !id.equals(own))
            .findFirst().map(id -> label((String)id)).orElse("Conversación");
    }

    private void clearThread() {
        current = null;
        cursor = "0";
        hasMore = false;
        seen.clear();
        messages.removeAll();
        messages.revalidate();
        messages.repaint();
        title.setText("Elige una conversación");
        secureButton.setVisible(false);
        shareButton.setVisible(false);
        send.setEnabled(false);
        more.setVisible(false);
        members.setVisible(false);
        add.setVisible(false);
        remove.setVisible(false);
        mediaControls();
    }

    private void open(String id) {
        if (busy || id.equals(current)) return;
        var row = available.stream().filter(item -> id.equals(item.get("id"))).findFirst();
        if (row.isEmpty()) return;
        clearThread();
        current = id;
        if (Boolean.TRUE.equals(row.get().get("e2ee"))) encrypted.add(id);
        secureButton.setVisible(true);
        shareButton.setVisible(encrypted.contains(id));
        title.setText(conversationLabel(row.get()));
        boolean group = "GROUP".equals(row.get().get("type"));
        members.setVisible(group);
        add.setVisible(group && own.equals(row.get().get("owner")));
        remove.setVisible(add.isVisible());
        send.setEnabled(true);
        mediaControls();
        page("history");
    }

    private void page(String op) {
        if (current == null) return;
        var request = new NetworkClient.Request(op, Map.of("conversation", current, "after", cursor, "limit", 40));
        String conversation = current;
        work(() -> {
            Response response = call(request);
            var payload = Json.map(response.result());
            if (encrypted.contains(conversation)) markLocked(conversation);
            for (var message : Json.rows(payload.get("messages"))) {
                if ("E2EE".equals(message.get("type")) || "E2EE_FILE".equals(message.get("type"))) {
                    markLocked(conversation);
                    try {
                        String plain = decrypt(message, conversation);
                        if ("E2EE_FILE".equals(message.get("type"))) {
                            var ticket = Json.map(Json.decode(plain));
                            var stored = Json.map(message.get("attachment"));
                            if (!stored.get("sha256").equals(ticket.get("sha256")) ||
                                !MediaTransfer.validName((String)ticket.get("filename"))) {
                                throw new java.security.GeneralSecurityException("Metadatos del archivo alterados");
                            }
                            message.put("ticket", ticket);
                        } else message.put("text", plain);
                    } catch (Exception error) {
                        message.remove("attachment");
                        message.put("text", "Contenido cifrado · " + error.getMessage());
                    }
                }
            }
            return response;
        }, result -> {
            var page = Json.map(result);
            for (var message : Json.rows(page.get("messages"))) {
                if (!seen.add((String)message.get("id"))) continue;
                if (message.get("attachment") instanceof Map<?, ?>) {
                    addAttachment(message);
                    messages.add(Box.createVerticalStrut(5));
                    continue;
                }
                JTextArea tile = new JTextArea(label((String)message.get("sender")) + "\n" + message.get("text"));
                tile.setEditable(false);
                tile.setLineWrap(true);
                tile.setWrapStyleWord(true);
                tile.setBackground(new Color(own.equals(message.get("sender")) ? 0xccfbf1 : 0xe2e8f0));
                tile.setBorder(BorderFactory.createEmptyBorder(6, 8, 6, 8));
                messages.add(tile);
                messages.add(Box.createVerticalStrut(5));
            }
            cursor = (String)page.get("next_cursor");
            hasMore = Boolean.TRUE.equals(page.get("more"));
            more.setVisible(hasMore);
            messages.revalidate();
            messages.repaint();
        });
    }

    private Path vault(String name) throws Exception { return vault(own, name); }

    private Path vault(String account, String name) throws Exception {
        if (account == null || !account.matches("[0-9a-f]{32}") || !name.matches("[a-z0-9.-]+")) {
            throw new IllegalStateException("Cuenta o nombre local inválido");
        }
        Path path = Path.of(System.getProperty("dog.e2ee.home",
            Path.of(System.getProperty("user.home"), ".dog-messenger", "e2ee").toString()), account);
        return path.resolve(name);
    }

    private void markLocked(String conversation) throws Exception {
        Path marker = vault("locked-" + conversation);
        Files.createDirectories(marker.getParent());
        Files.writeString(marker, "E1");
    }

    private void identityDialog() {
        if (busy || own == null) return;
        JPasswordField field = new JPasswordField();
        if (JOptionPane.showConfirmDialog(frame, new Object[]{"Clave local de la identidad (12 caracteres o más)", field},
                "Identidad de dispositivo", JOptionPane.OK_CANCEL_OPTION) != JOptionPane.OK_OPTION) return;
        char[] passphrase = field.getPassword();
        field.setText("");
        work(() -> {
            try {
                Path file = vault("device.bin");
                Files.createDirectories(file.getParent());
                E2ee.Device device = E2ee.store(file, passphrase, own);
                var keys = device.publicKeys();
                String id = UUID.randomUUID().toString().replace("-", "");
                var registration = new NetworkClient.Request("device-register", Map.of("device", keys.id(),
                        "exchange", keys.exchange(), "signing", keys.signing(), "proof", E2ee.proof(device, id, session)), id);
                return List.of(List.of(device, E2ee.fingerprint(keys.exchange(), keys.signing())), call(registration));
            } finally { Arrays.fill(passphrase, '\0'); }
        }, result -> {
            var values = (List<?>)result;
            var state = Json.map(values.get(1));
            if (!applied(state)) { report(state); return; }
            var local = (List<?>)values.get(0);
            localDevice = (E2ee.Device)local.get(0);
            JOptionPane.showMessageDialog(frame, "ID del dispositivo: " + localDevice.publicKeys().id()
                + "\nHuella SHA-256: " + local.get(1), "Identidad de dispositivo", JOptionPane.PLAIN_MESSAGE);
            if (current != null) {
                String selected = current;
                clearThread();
                open(selected);
            }
        });
    }

    private record Peer(String user, E2ee.Keys keys, String fingerprint, boolean known) {}

    private List<Peer> peers(Map<String, Object> directory) throws Exception { return peers(directory, own, localDevice); }

    private List<Peer> peers(Map<String, Object> directory, String account, E2ee.Device local) throws Exception {
        List<Peer> peers = new java.util.ArrayList<>();
        for (var row : Json.rows(directory.get("devices"))) {
            String user = (String)row.get("user"), id = (String)row.get("id");
            String exchange = (String)row.get("exchange"), signing = (String)row.get("signing");
            String fingerprint = E2ee.fingerprint(exchange, signing);
            if (!fingerprint.substring(0, 32).equals(id)) throw new java.security.GeneralSecurityException("Identidad alterada");
            var keys = new E2ee.Keys(id, exchange, signing);
            if (account.equals(user) && local != null && local.publicKeys().id().equals(id)) {
                peers.add(new Peer(user, keys, fingerprint, true));
                continue;
            }
            Path file = vault(account, "pin-" + user + "-" + id);
            boolean exists = Files.isRegularFile(file);
            if (exists && !Files.readString(file).equals(fingerprint)) {
                throw new java.security.GeneralSecurityException("Cambió la huella del usuario " + user);
            }
            peers.add(new Peer(user, keys, fingerprint, exists));
        }
        return peers;
    }

    private void configureSecure() {
        if (localDevice == null || current == null) {
            feedback.setText("Desbloquea primero la identidad de dispositivo");
            return;
        }
        String conversation = current;
        work(() -> {
            Response reply = call(new NetworkClient.Request("e2ee-directory", Map.of("conversation", conversation)));
            return List.of(peers(Json.map(reply.result())), reply);
        }, value -> {
            var pair = (List<?>)value;
            var directory = Json.map(pair.get(1));
            var confirmed = new java.util.ArrayList<Peer>();
            for (Object entry : (List<?>)pair.get(0)) {
                Peer peer = (Peer)entry;
                if (peer.known()) continue;
                if (JOptionPane.showConfirmDialog(frame, "Compara por un canal independiente la huella de " + label(peer.user())
                    + "\n" + peer.fingerprint() + "\n¿Coincide?", "Verificar huella", JOptionPane.YES_NO_OPTION)
                    != JOptionPane.YES_OPTION) return;
                confirmed.add(peer);
            }
            boolean active = Boolean.TRUE.equals(directory.get("enabled"));
            if (!active && JOptionPane.showConfirmDialog(frame, "¿Activar el cifrado para mensajes nuevos?",
                    "Cifrado de la conversación", JOptionPane.YES_NO_OPTION) != JOptionPane.YES_OPTION) return;
            work(() -> {
                for (Peer peer : confirmed) {
                    Files.writeString(vault("pin-" + peer.user() + "-" + peer.keys().id()), peer.fingerprint(), StandardOpenOption.CREATE_NEW);
                }
                Response reply = active ? null : call(new NetworkClient.Request("e2ee-enable", Map.of("conversation", conversation)));
                if (reply == null || applied(Json.map(reply.result()))) {
                    markLocked(conversation);
                    return new Response(Boolean.TRUE, reply == null ? connection.getClientProperty("algorithm").toString() : reply.algorithm(),
                                        reply == null ? minimum : reply.cursor());
                }
                return new Response(Boolean.FALSE, reply.algorithm(), reply.cursor());
            }, success -> {
                if (!Boolean.TRUE.equals(success)) { feedback.setText("Activación pendiente o rechazada"); return; }
                encrypted.add(conversation);
                clearThread();
                open(conversation);
                feedback.setText("Cifrado activado · huellas verificadas");
            });
        });
    }

    private void prepareMessage(String conversation, String text) {
        boolean secure = encrypted.contains(conversation);
        work(() -> {
            if (!secure && !Files.exists(vault("locked-" + conversation))) {
                return new Response(new NetworkClient.Request("send", Map.of("conversation", conversation, "text", text)),
                                    (String)connection.getClientProperty("algorithm"), minimum);
            }
            if (localDevice == null) throw new java.security.GeneralSecurityException("Desbloquea la identidad del dispositivo");
            var reply = call(new NetworkClient.Request("e2ee-directory", Map.of("conversation", conversation)));
            var directory = Json.map(reply.result());
            if (!Boolean.TRUE.equals(directory.get("enabled"))) {
                throw new java.security.GeneralSecurityException("El servidor cambió el modo de cifrado");
            }
            var recipients = peers(directory);
            if (recipients.stream().anyMatch(peer -> !peer.known())) {
                throw new java.security.GeneralSecurityException("Verifica las huellas antes de enviar");
            }
            String id = UUID.randomUUID().toString().replace("-", "");
            String epoch = (String)directory.get("epoch"), device = localDevice.publicKeys().id();
            var context = new E2ee.Context(conversation, id, device, epoch);
            String box = E2ee.seal(text, context, localDevice, recipients.stream().map(Peer::keys).toList());
            var request = new NetworkClient.Request("e2ee-send", Map.of("conversation", conversation, "device", device,
                                                    "epoch", epoch, "envelope", box), id);
            return new Response(request, reply.algorithm(), reply.cursor());
        }, value -> {
            pending = (NetworkClient.Request)value;
            submitMessage();
        });
    }

    private E2ee.Context messageContext(Map<String, Object> message, String conversation) {
        return new E2ee.Context(conversation, (String)message.get("id"), (String)message.get("device"),
            (String)message.get("epoch"), "E2EE_FILE".equals(message.get("type")) ? "file" : "text");
    }

    private E2ee.Keys verifiedSender(String user, String id, Map<String, Object> fields) throws Exception {
        var sender = new E2ee.Keys(id, (String)fields.get("exchange"), (String)fields.get("signing"));
        String mark = E2ee.fingerprint(sender.exchange(), sender.signing());
        if (!mark.substring(0, 32).equals(id)) throw new java.security.GeneralSecurityException("Identidad alterada");
        if (!(own.equals(user) && localDevice.publicKeys().id().equals(id))) {
            Path pin = vault("pin-" + user + "-" + id);
            if (!Files.isRegularFile(pin) || !Files.readString(pin).equals(mark)) {
                throw new java.security.GeneralSecurityException("Huella del emisor sin verificar");
            }
        }
        return sender;
    }

    private String decrypt(Map<String, Object> message, String conversation) throws Exception {
        if (localDevice == null) throw new java.security.GeneralSecurityException("Desbloquea la identidad local");
        var context = messageContext(message, conversation);
        var sender = verifiedSender((String)message.get("sender"), context.device(), Json.map(message.get("sender_keys")));
        String key = null;
        if (message.get("history_key") instanceof Map<?, ?>) {
            var grant = Json.map(message.get("history_key"));
            String issuer = (String)grant.get("issuer");
            var source = verifiedSender(own, issuer, grant);
            var proof = new E2ee.Context(conversation, context.message(), issuer, context.epoch(), "grant");
            key = E2ee.open((String)grant.get("envelope"), proof, source, localDevice);
        }
        return E2ee.open((String)message.get("envelope"), context, sender, localDevice, key);
    }

    private void revokeDevice() {
        if (busy || session == null) return;
        work(() -> call(new NetworkClient.Request("devices", Map.of())), result -> {
            var items = Json.rows(result).stream().filter(row -> Boolean.TRUE.equals(row.get("active")))
                .map(row -> (String)row.get("id")).toArray(String[]::new);
            if (items.length == 0) return;
            Object target = JOptionPane.showInputDialog(frame, "Dispositivo que se revocará", "Dispositivos vinculados",
                JOptionPane.QUESTION_MESSAGE, null, items, items[0]);
            if (target instanceof String id && JOptionPane.showConfirmDialog(frame, "¿Revocar " + id + " y sus sesiones?",
                    "Revocación irreversible", JOptionPane.YES_NO_OPTION) == JOptionPane.YES_OPTION) {
                mutate("device-revoke", Map.of("device", id), reply -> {
                    if (applied(reply) && localDevice != null && localDevice.publicKeys().id().equals(id)) showLogin();
                });
            }
        });
    }

    private void linkDialog() {
        if (busy || own == null) return;
        String[] options = {"Generar QR", "Autorizar descriptor", "Estado del vínculo", "Cancelar vínculo"};
        int answer = JOptionPane.showOptionDialog(frame, "Selecciona una operación", "Vincular dispositivo",
            JOptionPane.DEFAULT_OPTION, JOptionPane.QUESTION_MESSAGE, null, options, options[0]);
        if (answer == 0) createLink();
        if (answer == 1) approveLink();
        if (answer == 2 || answer == 3) {
            work(() -> {
                String link = Files.readString(vault("link-id"));
                return call(new NetworkClient.Request(answer == 2 ? "link-info" : "link-cancel", Map.of("link", link)));
            }, result -> {
                var state = Json.map(result);
                if (answer == 2) feedback.setText("Vínculo · " + state.get("status") +
                    ("LINKED".equals(state.get("status")) ? " · desbloquea la identidad de dispositivo" : ""));
                else report(state);
            });
        }
    }

    private void createLink() {
        JPasswordField field = new JPasswordField();
        if (JOptionPane.showConfirmDialog(frame, new Object[]{"Clave local de la nueva identidad", field},
                "Solicitar vínculo", JOptionPane.OK_CANCEL_OPTION) != JOptionPane.OK_OPTION) return;
        char[] passphrase = field.getPassword();
        field.setText("");
        work(() -> {
            try {
                Path file = vault("device.bin");
                Files.createDirectories(file.getParent());
                var device = E2ee.store(file, passphrase, own);
                Path pendingLink = vault("link-id");
                if (Files.isRegularFile(pendingLink)) {
                    String id = Files.readString(pendingLink);
                    var found = call(new NetworkClient.Request("link-info", Map.of("link", id)));
                    if ("PENDING".equals(Json.map(found.result()).get("status"))) {
                        var qr = call(new NetworkClient.Request("link-qr", Map.of("link", id)));
                        return List.of(E2ee.fingerprint(device.publicKeys().exchange(), device.publicKeys().signing()), qr);
                    }
                }
                var keys = device.publicKeys();
                String challenge = java.util.HexFormat.of().formatHex(E2ee.random(32));
                String issued = Long.toString(java.time.Instant.now().getEpochSecond());
                String id = UUID.randomUUID().toString().replace("-", "");
                var operation = new NetworkClient.Request("link-start", Map.of("device", keys.id(),
                    "exchange", keys.exchange(), "signing", keys.signing(), "challenge", challenge, "issued", issued,
                    "proof", E2ee.signLink(device, "start", id, session, challenge, issued)), id);
                Response response = call(operation);
                if (!applied(Json.map(response.result()))) throw new java.io.IOException("Solicitud rechazada o pendiente");
                Files.writeString(pendingLink, id);
                var qr = call(new NetworkClient.Request("link-qr", Map.of("link", id)));
                return List.of(E2ee.fingerprint(keys.exchange(), keys.signing()), qr);
            } finally { Arrays.fill(passphrase, '\0'); }
        }, value -> {
            var pair = (List<?>)value;
            var data = Json.map(pair.get(1));
            JTextArea descriptor = new JTextArea((String)data.get("descriptor"), 5, 32);
            descriptor.setLineWrap(true);
            descriptor.setEditable(false);
            JLabel qr = new JLabel("Descriptor público");
            if (data.get("png") instanceof String png) qr.setIcon(new ImageIcon(E2ee.raw(png)));
            JOptionPane.showMessageDialog(frame, new Object[]{qr, "Huella SHA-256: " + pair.get(0), new JScrollPane(descriptor)},
                                          "QR de vinculación", JOptionPane.PLAIN_MESSAGE);
        });
    }

    private void approveLink() {
        if (localDevice == null) { feedback.setText("Desbloquea la identidad autorizante"); return; }
        String text = JOptionPane.showInputDialog(frame, "Pega el descriptor público leído del QR");
        if (text == null || text.length() > 512) return;
        work(() -> {
            var descriptor = Json.map(Json.decode(text));
            if (!Long.valueOf(1).equals(descriptor.get("v"))) throw new java.io.IOException("Descriptor inválido");
            var reply = call(new NetworkClient.Request("link-info", Map.of("link", descriptor.get("id"))));
            var link = Json.map(reply.result());
            for (String field : List.of("id", "device", "challenge", "expires")) {
                if (!java.util.Objects.equals(link.get(field), descriptor.get(field))) {
                    throw new java.io.IOException("El descriptor cambió");
                }
            }
            String mark = E2ee.fingerprint((String)link.get("exchange"), (String)link.get("signing"));
            if (!"PENDING".equals(link.get("status")) || !mark.substring(0, 32).equals(link.get("device"))) {
                throw new java.io.IOException("Vínculo indisponible");
            }
            return List.of(mark, reply);
        }, value -> {
            var pair = (List<?>)value;
            String mark = (String)pair.get(0);
            var link = Json.map(pair.get(1));
            if (JOptionPane.showConfirmDialog(frame, "Compara la huella y autoriza explícitamente:\n" + mark,
                    "Vínculo", JOptionPane.YES_NO_OPTION) != JOptionPane.YES_OPTION) return;
            work(() -> {
                String id = UUID.randomUUID().toString().replace("-", "");
                String observed = Long.toString(java.time.Instant.now().getEpochSecond());
                String linkId = (String)link.get("id"), newDevice = (String)link.get("device");
                var req = new NetworkClient.Request("link-approve", Map.of("link", linkId, "device", localDevice.publicKeys().id(),
                    "observed", observed, "proof", E2ee.signLink(localDevice, "approve", id, session,
                        linkId, newDevice, (String)link.get("challenge"), observed)), id);
                var response = call(req);
                if (applied(Json.map(response.result()))) {
                    Files.writeString(vault("pin-" + own + "-" + newDevice), mark);
                }
                return response;
            }, result -> report(Json.map(result)));
        });
    }

    private void shareHistory() {
        if (busy || localDevice == null || current == null) return;
        String destination = JOptionPane.showInputDialog(frame, "ID de destino verificado");
        if (destination == null || !destination.matches("[0-9a-f]{32}")) return;
        if (JOptionPane.showConfirmDialog(frame, "¿Compartir las claves de esta conversación con " + destination + "?",
                "Claves de historial", JOptionPane.YES_NO_OPTION) != JOptionPane.YES_OPTION) return;
        String conversation = current;
        work(() -> {
            Response info = call(new NetworkClient.Request("e2ee-directory", Map.of("conversation", conversation)));
            var row = peers(Json.map(info.result())).stream().filter(peer -> peer.known()
                && peer.user().equals(own) && peer.keys().id().equals(destination)).findFirst().orElseThrow();
            int sent = 0;
            String after = "0";
            while (true) {
                Response history = call(new NetworkClient.Request("history",
                    Map.of("conversation", conversation, "after", after, "limit", 50)));
                var data = Json.map(history.result());
                for (var message : Json.rows(data.get("messages"))) {
                    if (!Set.of("E2EE", "E2EE_FILE").contains(message.get("type"))) continue;
                    String envelope = (String)message.get("envelope");
                    String mine = localDevice.publicKeys().id();
                    String[] sections = envelope.split("\\.", -1);
                    if (sections.length != 5 || Arrays.stream(sections[3].split(";"))
                            .noneMatch(wrap -> wrap.startsWith(mine + ":"))) continue;
                    var context = messageContext(message, conversation);
                    var sender = verifiedSender((String)message.get("sender"), context.device(),
                                                Json.map(message.get("sender_keys")));
                    String secret = E2ee.exportKey(envelope, context, sender, localDevice);
                    String box = E2ee.seal(secret, new E2ee.Context(conversation, context.message(), mine,
                        context.epoch(), "grant"), localDevice, List.of(row.keys()));
                    var reply = call(new NetworkClient.Request("history-grant", Map.of("message", context.message(),
                        "device", mine, "target", destination, "envelope", box)));
                    var outcome = Json.map(reply.result());
                    if (applied(outcome)) sent++;
                    else if (!(outcome.get("application") instanceof Map<?, ?> app
                               && "HISTORY_GRANT_EXISTS".equals(app.get("reason")))) {
                        throw new java.io.IOException("Distribución pendiente o rechazada");
                    }
                }
                after = (String)data.get("next_cursor");
                if (!Boolean.TRUE.equals(data.get("more"))) return new Response(sent, history.algorithm(), history.cursor());
            }
        }, result -> feedback.setText("Claves compartidas · " + result));
    }

    private void mediaControls() {
        attach.setEnabled(current != null && upload == null && !mediaBusy);
        resumeFile.setVisible(upload != null);
        resumeFile.setEnabled(!mediaBusy);
        cancelFile.setVisible(upload != null && !upload.completing);
    }

    private void uploadFile() {
        if (mediaBusy || upload == null || session == null) return;
        var attempt = upload;
        var transfer = new MediaTransfer(network, session, minimum);
        E2ee.Device device = localDevice;
        String account = own;
        boolean protectedChat = encrypted.contains(attempt.conversation);
        mediaWork(progress -> {
            if (protectedChat || Files.exists(vault(account, "locked-" + attempt.conversation))) {
                if (device == null) throw new java.security.GeneralSecurityException("Desbloquea la identidad del dispositivo");
                var directory = transfer.directory(attempt.conversation);
                if (!Boolean.TRUE.equals(directory.get("enabled"))) throw new java.security.GeneralSecurityException("Cambió el modo cifrado");
                var devices = peers(directory, account, device);
                if (devices.stream().anyMatch(peer -> !peer.known())) {
                    throw new java.security.GeneralSecurityException("Verifica las huellas antes de adjuntar");
                }
                transfer.prepareEncrypted(attempt, device, (String)directory.get("epoch"),
                                          devices.stream().map(Peer::keys).toList());
            }
            var result = transfer.upload(attempt, progress);
            if (Set.of("COMMITTED", "REJECTED", "ABORTED", "CANCELLED").contains(result.get("status"))
                    && !attempt.path.equals(attempt.original)) Files.deleteIfExists(attempt.path);
            return result;
        }, reply -> {
            var result = Json.map(reply);
            String phase = String.valueOf(result.get("status"));
            if (Set.of("COMMITTED", "REJECTED", "ABORTED", "CANCELLED").contains(phase)) upload = null;
            fileStatus.setText(phase.equals("COMMITTED") ? "Archivo confirmado" :
                phase + (result.get("reason") == null ? "" : " · " + result.get("reason")));
            if (phase.equals("COMMITTED") && attempt.conversation.equals(current) && !hasMore) page("sync");
        });
    }

    private void addAttachment(Map<String, Object> message) {
        var attachment = Json.map(message.get("attachment"));
        var ticket = message.get("ticket") instanceof Map<?, ?> ? Json.map(message.get("ticket")) : null;
        var view = ticket == null ? attachment : ticket;
        String conversation = (String)message.get("conversation");
        String filename = (String)view.get("filename");
        String type = (String)view.get(ticket == null ? "content_type" : "type");
        var title = new JLabel(filename + " · " + view.get("size") + " bytes");
        title.putClientProperty("html.disable", Boolean.TRUE);
        JButton download = new JButton("Descargar");
        download.addActionListener(event -> {
            if (mediaBusy || session == null) return;
            var chooser = new JFileChooser();
            chooser.setSelectedFile(new java.io.File(filename));
            if (chooser.showSaveDialog(frame) != JFileChooser.APPROVE_OPTION) return;
            var path = chooser.getSelectedFile().toPath();
            if (chooser.getSelectedFile().exists() && JOptionPane.showConfirmDialog(frame, "¿Reemplazar el archivo?",
                    "Descargar", JOptionPane.YES_NO_OPTION) != JOptionPane.YES_OPTION) return;
            var transfer = new MediaTransfer(network, session, minimum);
            mediaWork(progress -> {
                if (ticket == null) transfer.download(attachment, path, progress);
                else transfer.downloadEncrypted(attachment, ticket, conversation, path, progress);
                return null;
            },
                      result -> fileStatus.setText("Integridad verificada"));
        });
        JPanel card = actions(title, download);
        if (("image/png".equals(type) || "image/jpeg".equals(type)) && ((Number)view.get("size")).longValue() <= 2_097_152) {
            JButton preview = new JButton("Vista previa");
            preview.addActionListener(event -> {
                if (mediaBusy || session == null) return;
                var transfer = new MediaTransfer(network, session, minimum);
                mediaWork(progress -> transfer.preview(attachment, ticket, conversation, progress), result ->
                    JOptionPane.showMessageDialog(frame, new JLabel((ImageIcon)result),
                                                  "Vista previa", JOptionPane.PLAIN_MESSAGE));
            });
            card.add(preview);
        }
        messages.add(card);
    }

    private record TransferProgress(int value, String label) {}

    @FunctionalInterface
    private interface MediaJob {
        Object execute(java.util.function.BiConsumer<Integer, String> progress) throws Exception;
    }

    private void mediaWork(MediaJob action, Consumer<Object> doneAction) {
        String owner = session;
        mediaBusy = true;
        mediaControls();
        fileProgress.setValue(0);
        fileProgress.setVisible(true);
        new SwingWorker<Object, TransferProgress>() {
            protected Object doInBackground() throws Exception {
                return action.execute((value, label) -> publish(new TransferProgress(value, label)));
            }
            protected void process(List<TransferProgress> values) {
                if (!java.util.Objects.equals(owner, session)) return;
                var step = values.get(values.size() - 1);
                fileProgress.setValue(step.value());
                fileStatus.setText(step.label());
                mediaControls();
            }
            protected void done() {
                mediaBusy = false;
                mediaControls();
                if (!java.util.Objects.equals(owner, session)) return;
                try {
                    doneAction.accept(get());
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException error) {
                    fileStatus.setText(error.getCause().getMessage());
                }
                mediaControls();
            }
        }.execute();
    }

    private void poll() {
        if (busy || session == null || !frame.isShowing()) return;
        if (++ticks % 4 == 0 || current == null) refresh(ticks % 8 == 0);
        else if (!hasMore) page("sync");
    }

    private record GatewayErrorData(String message, String algorithm) {}
    private static final class GatewayError extends Exception {
        final GatewayErrorData data;
        GatewayError(GatewayErrorData data) { super(data.message()); this.data = data; }
    }

    private record Response(Object result, String algorithm, String cursor) {}

    private Response call(NetworkClient.Request request) throws Exception {
        var reply = network.call(request, session, minimum);
        String algorithm = (String)reply.get("algorithm");
        if (!Boolean.TRUE.equals(reply.get("ok"))) throw new GatewayError(new GatewayErrorData(
            (String)reply.get("error"), algorithm));
        return new Response(reply.get("result"), algorithm, (String)reply.get("cursor"));
    }

    private Response latest(Response first, Response second) {
        return Long.parseLong(first.cursor()) > Long.parseLong(second.cursor()) ? first : second;
    }

    private void work(Callable<?> job, Consumer<Object> success) {
        if (busy) return;
        busy = true;
        conversations.setEnabled(false);
        send.setEnabled(false);
        retry.setEnabled(false);
        new SwingWorker<Object, Void>() {
            protected Object doInBackground() throws Exception { return job.call(); }
            protected void done() {
                busy = false;
                conversations.setEnabled(true);
                send.setEnabled(current != null);
                retry.setEnabled(true);
                try {
                    Object value = get();
                    if (value instanceof Response response) {
                        update(response);
                        success.accept(response.result());
                    } else if (value instanceof List<?> pair && pair.get(1) instanceof Response response) {
                        if (pair.get(0) instanceof Response first) {
                            update(latest(first, response));
                            success.accept(List.of(first.result(), response.result()));
                        } else {
                            update(response);
                            success.accept(List.of(pair.get(0), response.result()));
                        }
                    }
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException error) {
                    Throwable cause = error.getCause();
                    if (cause instanceof GatewayError remote) {
                        connection.setText("Conectado · " + remote.data.algorithm().toUpperCase(java.util.Locale.ROOT));
                        if ("AUTH_REQUIRED".equals(cause.getMessage())) showLogin();
                    } else {
                        connection.setText("Desconectado");
                    }
                    feedback.setText(cause.getMessage());
                }
            }
        }.execute();
    }

    private void update(Response response) {
        connection.putClientProperty("algorithm", response.algorithm());
        connection.setText("Conectado · " + response.algorithm().toUpperCase(java.util.Locale.ROOT));
        minimum = Long.toString(Math.max(Long.parseLong(minimum), Long.parseLong(response.cursor())));
    }

    private boolean applied(Map<String, Object> result) {
        return result.get("application") instanceof Map<?, ?> app && "APPLIED".equals(app.get("status"));
    }

    private boolean report(Map<String, Object> result) {
        if (applied(result)) feedback.setText("Confirmado");
        else if (result.get("application") instanceof Map<?, ?> app && "REJECTED".equals(app.get("status"))) {
            feedback.setText("Rechazado · " + app.get("reason"));
            if ("AUTH_REQUIRED".equals(app.get("reason"))) showLogin();
        } else if ("ABORTED".equals(result.get("consensus"))) feedback.setText("Abortado");
        else if ("CONFLICT".equals(result.get("consensus"))) feedback.setText("Rechazado · identidad en conflicto");
        else {
            feedback.setText("Pendiente o indeterminado");
            return true;
        }
        return false;
    }
}
