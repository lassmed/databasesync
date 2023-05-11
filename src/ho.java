import java.io.IOException;
import java.sql.*;
import java.util.Arrays;

import com.rabbitmq.client.*;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;

public class ho {

    private static DefaultTableModel tableModel;
    private static final String[] COLUMN_NAMES = {"ID", "Product", "Qty", "Cost", "Amt", "Tax", "Total", "Date", "Region"};

    public static void main(String[] argv) throws Exception {

        // Set up the Swing GUI
        JFrame frame = new JFrame("Head Office");
        JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));

        JTable table = new JTable();
        tableModel = new DefaultTableModel(COLUMN_NAMES, 0);
        table.setModel(tableModel);
        JScrollPane scrollPane = new JScrollPane(table);
        panel.add(scrollPane);

        JPanel buttonPanel = new JPanel();
        buttonPanel.setLayout(new BoxLayout(buttonPanel, BoxLayout.X_AXIS));
        JButton refreshButton = new JButton("Refresh");
        refreshButton.addActionListener(e -> refreshTable());
        buttonPanel.add(refreshButton);
        buttonPanel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));
        panel.add(buttonPanel);

        frame.add(panel);
        frame.pack();
        frame.setVisible(true);


        // Set up MySQL database connection
        Class.forName("com.mysql.cj.jdbc.Driver");
        java.sql.Connection con = DriverManager.getConnection(
                "jdbc:mysql://localhost:3306/ho", "root", ""
        );

        // Set up RabbitMQ connection and channels
        String QUEUE_NAME_1 = "BO1_queue";
        String QUEUE_NAME_2 = "BO2_queue";
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        com.rabbitmq.client.Connection connection = factory.newConnection();

        Channel channel_1 = connection.createChannel();
        Channel channel_2 = connection.createChannel();

        // make queues durable
        boolean durable = true;
        channel_1.queueDeclare(QUEUE_NAME_1, durable, false, false, null);
        channel_2.queueDeclare(QUEUE_NAME_2, durable, false, false, null);

        // Consume messages from BO1 queue and insert into HO database
        Consumer consumer_1 = new DefaultConsumer(channel_1) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(message);
                String[] data = message.split(",");


                try {

                    PreparedStatement stmt = con.prepareStatement(
                            "INSERT INTO productsales ( product, qty, cost, amt, tax, total,date, region) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

                    stmt.setString(1, data[0]);
                    stmt.setInt(2, Integer.parseInt(data[1]));
                    stmt.setDouble(3, Double.parseDouble(data[2]));
                    stmt.setDouble(4, Double.parseDouble(data[3]));
                    stmt.setDouble(5, Double.parseDouble(data[4]));
                    stmt.setDouble(6, Double.parseDouble(data[5]));
                    stmt.setString( 7, data[6]);
                    stmt.setString(8, data[7]);

                    stmt.executeUpdate();
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        };


        // Consume messages from BO2 queue and insert into HO database
        Consumer consumer_2 = new DefaultConsumer(channel_2) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(message);
                String[] data = message.split(",");
                try {
                    PreparedStatement stmt = con.prepareStatement("INSERT INTO productsales ( product, qty, cost, amt, tax, total,date, region) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

                    stmt.setString(1, data[0]);
                    stmt.setInt(2, Integer.parseInt(data[1]));
                    stmt.setDouble(3, Double.parseDouble(data[2]));
                    stmt.setDouble(4, Double.parseDouble(data[3]));
                    stmt.setDouble(5, Double.parseDouble(data[4]));
                    stmt.setDouble(6, Double.parseDouble(data[5]));
                    stmt.setString( 7, data[6]);
                    stmt.setString(8, data[7]);

                    stmt.executeUpdate();
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        };

        channel_1.basicConsume(QUEUE_NAME_1, true, consumer_1);
        channel_2.basicConsume(QUEUE_NAME_2, true, consumer_2);

        // Keep main thread alive
        /*while (true) {
            Thread.sleep(1000);
        }*/
    }


    private static void refreshTable() {
        try {
            // Clear the table
            tableModel.setRowCount(0);

            // Set up MySQL database connection
            Class.forName("com.mysql.cj.jdbc.Driver");
            java.sql.Connection con = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/ho", "root", ""
            );

            // Retrieve data from MySQL database
            Statement statement = con.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from productsales");
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




}