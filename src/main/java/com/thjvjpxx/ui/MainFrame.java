package com.thjvjpxx.ui;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.EventQueue;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Map;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextField;
import javax.swing.JTextPane;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.UIManager;
import javax.swing.border.TitledBorder;
import javax.swing.plaf.FontUIResource;

import com.thjvjpxx.hadoop.CustomerClusteringRun;
import com.thjvjpxx.hadoop.CustomerFeature;
import com.thjvjpxx.service.MongoImportService;
import com.thjvjpxx.service.MySQLImportService;

public class MainFrame extends JFrame {

    private static final Font FONT = new Font("Arial", Font.BOLD, 18);
    private static final Font FONT_PLAIN = new Font("Arial", Font.PLAIN, 16);

    private int k = 3;
    private double threshold = 0.0001;
    private int iterations = 100;
    private Map<Integer, CustomerFeature> clusterResultFinal;
    private String selectedFilePath;

    private JButton btnReadCSV;
    private JButton btnAddMongoDB;
    private JButton btnAddMySQL;

    private JPanel mongoPanel;
    private JPanel mysqlPanel;

    // Thêm các trường thông tin cho MongoDB
    private JTextField txtMongoTotalRows;
    private JTextField txtMongoTotalTime;
    private JTextField txtMongoSpeed;

    // Thêm các trường thông tin cho MySQL
    private JTextField txtMySQLTotalRows;
    private JTextField txtMySQLTotalTime;
    private JTextField txtMySQLSpeed;

    private JTabbedPane tabbedPane;
    private JPanel comparePanel;
    private JPanel hadoopPanel;

    public static void setUIFont(Font font) {
        Enumeration<Object> keys = UIManager.getDefaults().keys();
        while (keys.hasMoreElements()) {
            Object key = keys.nextElement();
            Object value = UIManager.get(key);
            if (value instanceof FontUIResource) {
                UIManager.put(key, font);
            }
        }
    }

    public static void main(String[] args) {
        setUIFont(FONT);
        EventQueue.invokeLater(new Runnable() {
            public void run() {
                try {
                    MainFrame frame = new MainFrame();
                    frame.setVisible(true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public MainFrame() {
        setResizable(false);
        setTitle("Final Project BigData");
        setSize(1100, 350);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        getContentPane().setLayout(new BorderLayout());
        setLocationRelativeTo(null);

        // Tạo TabbedPane
        tabbedPane = new JTabbedPane();
        Font tabFont = new Font("Arial", Font.BOLD, 18);
        tabbedPane.setFont(tabFont);

        // Tạo panel cho tab "So sánh"
        comparePanel = new JPanel(new BorderLayout());

        // Di chuyển code hiện tại vào comparePanel
        JPanel buttonPanel = new JPanel();
        buttonPanel.setLayout(new FlowLayout());

        btnReadCSV = new JButton("Đọc file CSV");
        btnAddMongoDB = new JButton("Thêm vào MongoDB");
        btnAddMySQL = new JButton("Thêm vào MySQL");

        buttonPanel.add(btnReadCSV);
        buttonPanel.add(btnAddMongoDB);
        buttonPanel.add(btnAddMySQL);

        comparePanel.add(buttonPanel, BorderLayout.NORTH);

        JPanel mainPanel = new JPanel();
        mainPanel.setLayout(new GridLayout(1, 2, 10, 10));

        mongoPanel = createInfoPanel("MongoDB");
        mysqlPanel = createInfoPanel("MySQL");

        mainPanel.add(mongoPanel);
        mainPanel.add(mysqlPanel);

        comparePanel.add(mainPanel, BorderLayout.CENTER);

        btnReadCSV.setFont(FONT);
        btnAddMongoDB.setFont(FONT);
        btnAddMySQL.setFont(FONT);

        txtMongoTotalRows.setFont(FONT);
        txtMongoTotalTime.setFont(FONT);
        txtMongoSpeed.setFont(FONT);

        txtMySQLTotalRows.setFont(FONT);
        txtMySQLTotalTime.setFont(FONT);
        txtMySQLSpeed.setFont(FONT);

        // Tạo panel cho tab "Hadoop"
        hadoopPanel = createHadoopPanel();

        // Thêm các tab vào TabbedPane
        tabbedPane.addTab("So sánh", comparePanel);
        tabbedPane.addTab("Hadoop", hadoopPanel);

        // Thêm TabbedPane vào frame
        getContentPane().add(tabbedPane, BorderLayout.CENTER);

        // [Giữ nguyên các event listeners...]
        btnReadCSV.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                readCSVFile();
            }
        });

        btnAddMongoDB.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                addToMongoDB();
            }
        });

        btnAddMySQL.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                addToMySQL();
            }
        });
    }

    // Thêm phương thức mới để tạo panel Hadoop
    private JPanel createHadoopPanel() {
        JPanel panel = new JPanel(new BorderLayout());

        // Tạo panel cho các nút điều khiển
        JPanel buttonPanel = new JPanel(new FlowLayout());
        JButton btnProcessHadoop = new JButton("Process K-means");
        btnProcessHadoop.setFont(FONT);
        buttonPanel.add(btnProcessHadoop);

        // Tạo panel chính chứa 2 groupbox
        JPanel mainPanel = new JPanel();
        mainPanel.setLayout(new GridLayout(1, 2, 10, 10));

        // Tạo Input panel (giữ nguyên như cũ)
        JPanel inputPanel = new JPanel();
        inputPanel.setBorder(BorderFactory.createTitledBorder(
                BorderFactory.createEtchedBorder(),
                "Input Information",
                TitledBorder.LEFT,
                TitledBorder.TOP,
                FONT));
        inputPanel.setLayout(new GridBagLayout());

        // Labels và TextFields cho Input (giữ nguyên)
        JLabel lblClusters = new JLabel("Số cụm:");
        JLabel lblThreshold = new JLabel("Ngưỡng hội tụ:");
        JLabel lblIterations = new JLabel("Số lần lặp:");

        JTextField txtClusters = new JTextField(10);
        JTextField txtThreshold = new JTextField(10);
        JTextField txtIterations = new JTextField(10);

        // Set properties cho components Input
        lblClusters.setFont(FONT_PLAIN);
        lblThreshold.setFont(FONT_PLAIN);
        lblIterations.setFont(FONT_PLAIN);
        txtClusters.setFont(FONT_PLAIN);
        txtThreshold.setFont(FONT_PLAIN);
        txtIterations.setFont(FONT_PLAIN);

        txtClusters.setText(k + "");
        txtThreshold.setText(threshold + "");
        txtIterations.setText(iterations + "");

        // Add components vào Input panel
        addComponent(inputPanel, lblClusters, 0, 2, false);
        addComponent(inputPanel, txtClusters, 1, 2, true);
        addComponent(inputPanel, lblThreshold, 0, 3, false);
        addComponent(inputPanel, txtThreshold, 1, 3, true);
        addComponent(inputPanel, lblIterations, 0, 4, false);
        addComponent(inputPanel, txtIterations, 1, 4, true);

        // Tạo Output panel MỚI với JTextPane
        JPanel outputPanel = new JPanel(new BorderLayout());
        outputPanel.setBorder(BorderFactory.createTitledBorder(
                BorderFactory.createEtchedBorder(),
                "Output Information",
                TitledBorder.LEFT,
                TitledBorder.TOP,
                FONT));

        // Tạo JTextPane
        JTextPane textPane = new JTextPane();
        textPane.setContentType("text/html");
        textPane.setEditable(false);
        textPane.setFont(FONT_PLAIN);

        // Thêm JTextPane vào JScrollPane để có thanh cuộn
        JScrollPane scrollPane = new JScrollPane(textPane);
        outputPanel.add(scrollPane, BorderLayout.CENTER);

        // Thêm cả hai panel vào main panel
        mainPanel.add(inputPanel);
        mainPanel.add(outputPanel);

        // Thêm các panel vào panel chính
        panel.add(buttonPanel, BorderLayout.NORTH);
        panel.add(mainPanel, BorderLayout.CENTER);

        btnProcessHadoop.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                if (selectedFilePath == null || selectedFilePath.isEmpty()) {
                    JOptionPane.showMessageDialog(
                            MainFrame.this,
                            "Vui lòng chọn file CSV trước!",
                            "Thông báo",
                            JOptionPane.WARNING_MESSAGE);
                    return;
                }
                File file = new File(selectedFilePath);
                k = Integer.parseInt(txtClusters.getText());
                threshold = Double.parseDouble(txtThreshold.getText());
                iterations = Integer.parseInt(txtIterations.getText());
                String path = file.getParent() + "/output";
                CustomerClusteringRun clusteringRun = new CustomerClusteringRun(path, iterations,
                        threshold, k);
                // Dialog tiến trình
                JDialog progressDialog = new JDialog(MainFrame.this, "Đang xử lý", true);
                progressDialog.setLayout(new BorderLayout(10, 10));

                JLabel statusLabel = new JLabel("Đang thực hiện phân cụm...");
                statusLabel.setFont(FONT);
                statusLabel.setHorizontalAlignment(JLabel.CENTER);
                statusLabel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));

                JProgressBar progressBar = new JProgressBar();
                progressBar.setIndeterminate(true);
                progressBar.setPreferredSize(new Dimension(300, 30));

                JPanel panel = new JPanel();
                panel.setLayout(new BorderLayout(10, 10));
                panel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));
                panel.add(statusLabel, BorderLayout.CENTER);
                panel.add(progressBar, BorderLayout.SOUTH);

                progressDialog.add(panel);
                progressDialog.pack();
                progressDialog.setLocationRelativeTo(MainFrame.this);
                // Task xử lý trong thread riêng
                SwingWorker<Void, Void> worker = new SwingWorker<Void, Void>() {
                    @Override
                    protected Void doInBackground() throws Exception {
                        try {
                            clusterResultFinal = clusteringRun.processData();
                        } catch (Exception er) {
                            System.err.println("Đã xảy ra lỗi: " + er);
                            JOptionPane.showMessageDialog(
                                    MainFrame.this,
                                    "Đã xảy ra lỗi, vui lòng thử lại sau!",
                                    "Lỗi",
                                    JOptionPane.ERROR_MESSAGE);
                            er.printStackTrace();
                        }
                        return null;
                    }

                    @Override
                    protected void done() {
                        progressDialog.dispose();
                        try {
                            get(); // Kiểm tra exception
                            if (clusterResultFinal != null) {
                                StringBuilder sb = new StringBuilder();
                                for (Map.Entry<Integer, CustomerFeature> item : clusterResultFinal.entrySet()) {
                                    String[] splitRes = item.getValue().toString().split(",");
                                    sb.append("<h1>Cụm thứ ")
                                            .append(item.getKey() + ":</h1>")
                                            .append("<ul margin-top: 0px; list-style-type: disc'>")
                                            .append("<li style='font-size:14px; font-weight:normal'>Tổng chi tiêu: ")
                                            .append(splitRes[0] + "</li>")
                                            .append("<li style='font-size:14px; font-weight:normal'>Tần suất mua hàng: ")
                                            .append(splitRes[1] + "</li>")
                                            .append("<li style='font-size:14px; font-weight:normal'>Số lượng đơn hàng trung bình cho mỗi đơn: ")
                                            .append(splitRes[2] + "</li>")
                                            .append("</ul>");
                                }
                                SwingUtilities.invokeLater(() -> {
                                    textPane.setText(sb.toString());
                                });
                                JOptionPane.showMessageDialog(
                                        MainFrame.this,
                                        "Phân cụm thành công",
                                        "Thành công",
                                        JOptionPane.INFORMATION_MESSAGE);
                            } else {
                                String message = "Số vòng lặp không đủ để hội tụ!";
                                textPane.setText(String.format("<h1>%s</h1>", message));
                                JOptionPane.showMessageDialog(
                                        MainFrame.this,
                                        message,
                                        "Cảnh báo",
                                        JOptionPane.WARNING_MESSAGE);
                            }
                        } catch (Exception e) {
                            System.err.println("Đã xảy ra lỗi: " + e);
                            JOptionPane.showMessageDialog(
                                    MainFrame.this,
                                    e,
                                    "Lỗi",
                                    JOptionPane.ERROR_MESSAGE);
                            e.printStackTrace();
                        }
                    }
                };

                worker.execute();
                progressDialog.setVisible(true);
            }
        });

        return panel;
    }

    private JPanel createInfoPanel(String title) {
        JPanel panel = new JPanel();
        panel.setBorder(BorderFactory.createTitledBorder(title));
        panel.setLayout(new GridBagLayout());
        panel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(),
                title,
                TitledBorder.LEFT,
                TitledBorder.TOP,
                FONT));

        // Labels
        JLabel lblTotalRows = new JLabel("Tổng số dòng:");
        JLabel lblTotalTime = new JLabel("Tổng thời gian:");
        JLabel lblSpeed = new JLabel("Tốc độ ghi (dòng/giây):");

        // Set font cho labels
        lblTotalRows.setFont(FONT_PLAIN);
        lblTotalTime.setFont(FONT_PLAIN);
        lblSpeed.setFont(FONT_PLAIN);

        // Text Fields
        JTextField txtTotalRows = new JTextField(10);
        JTextField txtTotalTime = new JTextField(10);
        JTextField txtSpeed = new JTextField(10);

        // Thiết lập các text field là chỉ đọc
        txtTotalRows.setEditable(false);
        txtTotalTime.setEditable(false);
        txtSpeed.setEditable(false);

        // Thêm components vào panel
        addComponent(panel, lblTotalRows, 0, 0, false);
        addComponent(panel, txtTotalRows, 1, 0, true);

        addComponent(panel, lblTotalTime, 0, 1, false);
        addComponent(panel, txtTotalTime, 1, 1, true);

        addComponent(panel, lblSpeed, 0, 2, false);
        addComponent(panel, txtSpeed, 1, 2, true);

        // Lưu references nếu là MongoDB panel
        if (title.equals("MongoDB")) {
            txtMongoTotalRows = txtTotalRows;
            txtMongoTotalTime = txtTotalTime;
            txtMongoSpeed = txtSpeed;
        } else {
            txtMySQLTotalRows = txtTotalRows;
            txtMySQLTotalTime = txtTotalTime;
            txtMySQLSpeed = txtSpeed;
        }

        return panel;
    }

    private void addComponent(JPanel panel, Component component, int gridx, int gridy, boolean isTextField) {
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = gridx;
        gbc.gridy = gridy;
        gbc.insets = new Insets(5, 5, 5, 5);

        if (isTextField) {
            gbc.fill = GridBagConstraints.HORIZONTAL;
            gbc.weightx = 1.0;
        } else {
            gbc.anchor = GridBagConstraints.WEST;
        }

        panel.add(component, gbc);
    }

    private void readCSVFile() {
        JFileChooser fileChooser = new JFileChooser();
        int result = fileChooser.showOpenDialog(this);
        if (result == JFileChooser.APPROVE_OPTION) {
            selectedFilePath = fileChooser.getSelectedFile().getAbsolutePath();
            JOptionPane.showMessageDialog(
                    this,
                    "Đã chọn file: " + fileChooser.getSelectedFile().getName(),
                    "Thông báo",
                    JOptionPane.INFORMATION_MESSAGE);
        }
    }

    private boolean showImportDialog(String type, AutoCloseable service,
            ImportCallback importCallback, UpdateInfoCallback updateCallback) {
        try {
            // Kiểm tra file đã chọn
            if (selectedFilePath == null || selectedFilePath.isEmpty()) {
                JOptionPane.showMessageDialog(
                        this,
                        "Vui lòng chọn file CSV trước!",
                        "Thông báo",
                        JOptionPane.WARNING_MESSAGE);
                return false;
            }

            // Dialog tiến trình
            JDialog progressDialog = new JDialog(this, "Đang xử lý", true);
            progressDialog.setLayout(new BorderLayout(10, 10));

            JLabel statusLabel = new JLabel("Đang import vào " + type + "...");
            statusLabel.setFont(FONT);
            statusLabel.setHorizontalAlignment(JLabel.CENTER);
            statusLabel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));

            JProgressBar progressBar = new JProgressBar();
            progressBar.setIndeterminate(true);
            progressBar.setPreferredSize(new Dimension(300, 30));

            JPanel panel = new JPanel();
            panel.setLayout(new BorderLayout(10, 10));
            panel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));
            panel.add(statusLabel, BorderLayout.CENTER);
            panel.add(progressBar, BorderLayout.SOUTH);

            progressDialog.add(panel);
            progressDialog.pack();
            progressDialog.setLocationRelativeTo(this);

            // Task xử lý trong thread riêng
            SwingWorker<Void, Void> worker = new SwingWorker<Void, Void>() {
                private long recordCount;
                private double totalTimeSeconds;
                private double speed;

                @Override
                protected Void doInBackground() throws Exception {
                    try (AutoCloseable importService = service) {
                        long startTime = System.currentTimeMillis();

                        recordCount = importCallback.doImport();

                        long endTime = System.currentTimeMillis();
                        totalTimeSeconds = (endTime - startTime) / 1000.0;
                        speed = recordCount / totalTimeSeconds;
                    }
                    return null;
                }

                @Override
                protected void done() {
                    progressDialog.dispose();
                    try {
                        get(); // Kiểm tra exception

                        // Cập nhật thông tin lên GUI
                        updateCallback.updateInfo(
                                (int) recordCount,
                                totalTimeSeconds,
                                speed);

                        // Thông báo thành công
                        JOptionPane.showMessageDialog(
                                MainFrame.this,
                                String.format(
                                        "Import thành công!\nTổng số dòng: %d\nTổng thời gian: %.2fs\nTốc độ: %.2f dòng/giây",
                                        recordCount,
                                        totalTimeSeconds,
                                        speed),
                                "Thành công",
                                JOptionPane.INFORMATION_MESSAGE);

                    } catch (Exception e) {
                        String errorMessage;
                        Throwable cause = e.getCause();

                        if (cause instanceof SQLException) {
                            errorMessage = "Lỗi SQL: " + cause.getMessage();
                        } else if (cause instanceof IOException) {
                            errorMessage = "Lỗi đọc file: " + cause.getMessage();
                        } else {
                            errorMessage = "Lỗi không xác định: " + cause.getMessage();
                        }

                        JOptionPane.showMessageDialog(
                                MainFrame.this,
                                errorMessage,
                                "Lỗi",
                                JOptionPane.ERROR_MESSAGE);
                        e.printStackTrace();
                    }
                }
            };

            worker.execute();
            progressDialog.setVisible(true);
            return true;

        } catch (Exception e) {
            JOptionPane.showMessageDialog(
                    this,
                    "Lỗi không xác định: " + e.getMessage(),
                    "Lỗi",
                    JOptionPane.ERROR_MESSAGE);
            e.printStackTrace();
            return false;
        }
    }

    // Định nghĩa các interface callback
    @FunctionalInterface
    private interface ImportCallback {
        long doImport() throws Exception;
    }

    @FunctionalInterface
    private interface UpdateInfoCallback {
        void updateInfo(int totalRows, double totalTime, double speed);
    }

    private void addToMongoDB() {
        MongoImportService service = new MongoImportService();
        showImportDialog(
                "MongoDB",
                service,
                () -> {
                    service.initialize();
                    return service.importFromCSV(selectedFilePath);
                },
                this::updateMongoInfo);
    }

    private void addToMySQL() {
        MySQLImportService service = new MySQLImportService();
        showImportDialog(
                "MySQL",
                service,
                () -> {
                    service.initialize();
                    return service.importFromCSV(selectedFilePath);
                },
                this::updateMySQLInfo);
    }

    // Phương thức để cập nhật thông tin MongoDB
    public void updateMongoInfo(int totalRows, double totalTime, double speed) {
        txtMongoTotalRows.setText(String.valueOf(totalRows));
        txtMongoTotalTime.setText(String.format("%.2fs", totalTime));
        txtMongoSpeed.setText(String.format("%.2f", speed));
    }

    // Phương thức để cập nhật thông tin MySQL
    public void updateMySQLInfo(int totalRows, double totalTime, double speed) {
        txtMySQLTotalRows.setText(String.valueOf(totalRows));
        txtMySQLTotalTime.setText(String.format("%.2fs", totalTime));
        txtMySQLSpeed.setText(String.format("%.2f", speed));
    }
}
