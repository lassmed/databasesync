import com.rabbitmq.client.*;

import java.awt.*;
import java.sql.*;
import java.util.Arrays;
import java.util.concurrent.*;
import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.lang.*;

public class bo2 {


    private static final String QUEUE_NAME = "BO2_queue";
    private static final String[] COLUMN_NAMES = {"ID", "Product", "Qty", "Cost", "Amt", "Tax", "Total", "Date", "Region"};
    private static DefaultTableModel tableModel;

    public static void main(String[] args) {

        // Set up the Swing GUI
        JFrame frame = new JFrame("Branch Office 2");
        JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));

        JTable table = new JTable();
        tableModel = new DefaultTableModel(COLUMN_NAMES, 0);
        table.setModel(tableModel);
        JScrollPane scrollPane = new JScrollPane(table);
        panel.add(scrollPane);

        JButton sendButton = new JButton("Send");
        sendButton.addActionListener(e -> sendToRabbitMQ());

        JButton refreshButton = new JButton("Refresh");
        refreshButton.addActionListener(e -> refreshTable());

        JPanel buttonPanel = new JPanel();
        buttonPanel.setLayout(new BoxLayout(buttonPanel, BoxLayout.X_AXIS));
        buttonPanel.add(sendButton);
        buttonPanel.add(Box.createRigidArea(new Dimension(10, 0)));
        buttonPanel.add(refreshButton);
        buttonPanel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));

        panel.add(buttonPanel, BorderLayout.CENTER);
        frame.add(panel);
        frame.pack();
        frame.setVisible(true);

        // Synchronize once a day
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> refreshTable(), 0, 1, TimeUnit.DAYS);
    }

    private static void refreshTable() {
        try {
            // Clear the table
            tableModel.setRowCount(0);

            // Set up MySQL database connection
            Class.forName("com.mysql.cj.jdbc.Driver");
            java.sql.Connection con = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/bo2", "root", ""
            );

            // Retrieve data from MySQL database
            Statement statement = con.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from productsales where sent = 0");
            while (resultSet.next()) {
                Object[] rowData = {
                        resultSet.getInt("id"),
                        resultSet.getString("product"),
                        resultSet.getString("qty"),
                        resultSet.getString("cost"),
                        resultSet.getString("amt"),
                        resultSet.getString("tax"),
                        resultSet.getString("total"),
                        resultSet.getString("date"),
                        resultSet.getString("region")
                };
                System.out.println(Arrays.toString(rowData));
                tableModel.addRow(rowData);
            }

            // Close SQL connection
            resultSet.close();
            statement.close();
            con.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    private static void markAsSent(int id) {

        try {

            java.sql.Connection con = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/bo2", "root", ""
            );

            // Update sent flag to 1
            PreparedStatement updateStmt = con.prepareStatement("UPDATE productsales SET sent = 1 WHERE id = ?");
            updateStmt.setInt(1, id);
            updateStmt.executeUpdate();

            updateStmt.close();
            con.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }




    private static void sendToRabbitMQ() {
        try {
            // Set up RabbitMQ connection and channel
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            com.rabbitmq.client.Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            // Make queue persistent
            boolean durable = true;
            channel.queueDeclare(QUEUE_NAME, durable, false, false, null);

            // Send each row in the table to RabbitMQ
            for (int i = 0; i < tableModel.getRowCount(); i++) {
                String message =
                        tableModel.getValueAt(i, 1) + "," +
                                tableModel.getValueAt(i, 2) + "," +
                                tableModel.getValueAt(i, 3) + "," +
                                tableModel.getValueAt(i, 4) + "," +
                                tableModel.getValueAt(i, 5) + "," +
                                tableModel.getValueAt(i, 6) + "," +
                                tableModel.getValueAt(i, 7) + "," +
                                tableModel.getValueAt(i, 8)+ ",";
                System.out.println(message);

                //publish row in queue
                AMQP.BasicProperties props = MessageProperties.PERSISTENT_TEXT_PLAIN;
                channel.basicPublish("", QUEUE_NAME, props, message.getBytes("UTF-8"));

                // Update sent flag to 1
                int id = (int) tableModel.getValueAt(i, 0);
                markAsSent(id);


            }

            //close rabbitmq
            channel.close();
            connection.close();


        } catch (Exception e) {
            System.out.println(e);
        }
    }



}