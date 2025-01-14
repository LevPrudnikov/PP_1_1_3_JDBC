package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {
    public UserDaoJDBCImpl() {
    }

    public void createUsersTable() {
        String sql = "CREATE TABLE IF NOT EXISTS users (id INT NOT NULL AUTO_INCREMENT,name VARCHAR(45) NOT NULL, " +
                "lastname VARCHAR(45) NOT NULL, age TINYINT(3) NULL, " +
                "PRIMARY KEY (`id`))";
        Connection connection = Util.getConnection();
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
            connection.commit();
        } catch (SQLException ignore) {
            try {
                connection.rollback();
            } catch (SQLException ignored) {}
        }
        try {
            connection.close();
        } catch (SQLException ignore) {}
    }

    public void dropUsersTable() {
        String sql = "DROP TABLE IF EXISTS users";
        Connection connection = Util.getConnection();
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
            connection.commit();
        } catch (SQLException ignore) {
            try {
                connection.rollback();
            } catch (SQLException ignored) {}
        }
        try {
            connection.close();
        } catch (SQLException ignore) {}
    }

    public void saveUser(String name, String lastName, byte age) {

        String sql = "INSERT INTO users (name, lastname, age) VALUES (?, ?, ?)";
        Connection connection = Util.getConnection();
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setInt(3, age);
            preparedStatement.executeUpdate();
            connection.commit();
        } catch (SQLException ignore) {
            try {
                connection.rollback();
            } catch (SQLException ignored) {}
        }
        try {
            connection.close();
        } catch (SQLException ignore) {}
    }

    public void removeUserById(long id) {
        String sql = "DELETE FROM users WHERE id = ?";
        Connection connection = Util.getConnection();
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setLong(1, id);
            preparedStatement.executeUpdate();
            connection.commit();
        } catch (SQLException ignore) {
            try {
                connection.rollback();
            } catch (SQLException ignored) {}
        }
        try {
            connection.close();
        } catch (SQLException ignore) {}
    }

    public List<User> getAllUsers() {
        List<User> users = new ArrayList<>();
        String sql = "SELECT * FROM users";
        Connection connection = Util.getConnection();
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                User user = new User();
                user.setId(resultSet.getLong("id"));
                user.setName(resultSet.getString("name"));
                user.setLastName(resultSet.getString("lastname"));
                user.setAge((byte) resultSet.getInt("age"));

                users.add(user);
            }
            connection.commit();
        } catch (SQLException ignore) {
            try {
                connection.rollback();
            } catch (SQLException ignored) {}
        }
        try {
            connection.close();
        } catch (SQLException ignore) {}
        return users;
    }

    public void cleanUsersTable() {
        String sql = "TRUNCATE TABLE users";
        Connection connection = Util.getConnection();
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
            connection.commit();
        } catch (SQLException ignore) {
            try {
                connection.rollback();
            } catch (SQLException ignored) {}
        }
        try {
            connection.close();
        } catch (SQLException ignore) {}
    }
}
