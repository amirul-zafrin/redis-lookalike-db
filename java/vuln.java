import java.io.*;
import java.sql.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.util.Base64;
import java.util.logging.*;

public class VulnerableApp extends HttpServlet {

    public void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
        PrintWriter out = res.getWriter();

        // 1. SQL Injection
        String user = req.getParameter("username");
        String pass = req.getParameter("password");
        try {
            Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/test", "root", "root");
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM users WHERE username='" + user + "' AND password='" + pass + "'");
            if (rs.next()) {
                
                // 2. Broken Authentication
                if (user.equals("admin") && pass.equals("admin123")) {
                    out.println("Welcome admin!");

                    // 3. Sensitive Data Exposure
                    FileWriter fw = new FileWriter("secrets.txt");
                    fw.write("User: " + user + ", Password: " + pass);
                    fw.close();
                }

                // 4. Insecure Deserialization
                ObjectInputStream ois = new ObjectInputStream(req.getInputStream());
                Object obj = ois.readObject(); // deserializing unknown object

                // 5. XSS
                String comment = req.getParameter("comment");
                out.println("<p>Comment: " + comment + "</p>");

                // 6. Security Misconfiguration
                throw new Exception("Debug info: user=" + user);

            } else {
                // 9. No logging for failed login
                out.println("Login failed.");
            }

        } catch (Exception e) {
            // Exposes internal error (stack trace)
            e.printStackTrace(out);
        }

        // 7. Broken Access Control
        if ("1".equals(req.getParameter("delete"))) {
            deleteUserAccount(); // no role check
        }

        // 8. Using Vulnerable Components - simulated via comment
        // Using commons-collections:3.2.1 (known vulnerable)

        // 10. CSRF - no protection
        out.println("<form method='POST' action='/transfer'>");
        out.println("<input name='amount' value='1000'><input type='submit'>");
        out.println("</form>");
    }

    private void deleteUserAccount() {
        System.out.println("User deleted!");
    }
}
