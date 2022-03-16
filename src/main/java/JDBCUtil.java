import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class JDBCUtil {
    String URL, user, password;
    Connection connection;
    public JDBCUtil(String user, String password, String DBName) {
        this.URL = "jdbc:postgresql://localhost:5432/" + DBName;
        this.user = user;
        this.password = password;
        this.connection = null;
    }

    public void getConnection() {
        // Load the driver class
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException ex) {
            System.out.println("Unable to load the class. Terminating the program");
            ex.printStackTrace();
            System.exit(-1);
        }
        // Get the connection
        try {
            // Set postgres properties
            Properties props = new Properties();
            props.setProperty("user", user);
            props.setProperty("password", password);
            connection = DriverManager.getConnection(URL, props);
        } catch (SQLException ex) {
            System.out.println("Error getting connection: " + ex.getMessage());
            System.exit(-1);
        } catch (Exception ex) {
            System.out.println("Error: " + ex.getMessage());
            System.exit(-1);
        }
    }

    public void printResultSet(ResultSet rs, int limit) throws SQLException {
        ResultSetMetaData rsData = rs.getMetaData();
        int columnsNumber = rsData.getColumnCount();
        List<String>[] columns = new ArrayList[columnsNumber + 1];
        for (int i = 0; i <= columnsNumber; i++) {
            columns[i] = new ArrayList<>();
        }

        int[] lengths = new int[columnsNumber + 1];
        columns[0].add("#");
        for (int i = 1; i <= columnsNumber; i++) {
            String columnName = rsData.getColumnName(i);
            columns[i].add(columnName);
            lengths[i] = columnName.length();
        }

        int count = 1;
        while (rs.next()) {
            columns[0].add(String.valueOf(count));
            for (int i = 1; i <= columnsNumber; i++) {
                String value = rs.getString(i);
                value = value == null ? "null" : value;
                columns[i].add(value);
                lengths[i] = Math.max(lengths[i], value.length());
            }
            if (count > limit) break;
            count++;
        }

        lengths[0] = String.valueOf(count).length();

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < Math.min(count, limit); i++) {
            for (int j = 0; j <= columnsNumber; j++) {
                sb.append(String.format(" %-" + lengths[j] + "s ", columns[j].get(i)));
            }
            sb.append("\n");
        }

        System.out.print(sb.toString());
    }

    public void query() throws SQLException {
        // Get the connection
        this.getConnection();

        // Specify the query
        String query = "SELECT id, name FROM person_100k WHERE name like ? LIMIT ?";

        PreparedStatement statement = this.connection.prepareStatement(query);

        // Set the prepared statement parameters
        statement.setString(1, "Reeves%");
        statement.setInt(2, 20);

        // Execute the query
        ResultSet rs = statement.executeQuery();

        // Pretty print the result set
        printResultSet(rs, 100);

        // Close the statement
        rs.close();
        statement.close();
    }

    public static void main(String[] args) throws SQLException {
        // Create the connection
        // Put your credentials (postgres user, password) here
        JDBCUtil util = new JDBCUtil("postgres", "password", "imdb_100k");

        // Run the query
        util.query();
    }
}