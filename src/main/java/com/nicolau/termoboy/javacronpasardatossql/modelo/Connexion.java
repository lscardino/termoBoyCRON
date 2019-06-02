/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nicolau.termoboy.javacronpasardatossql.modelo;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Lorenzo
 */
public class Connexion {

    private String DRIVER_CLASS_NAME;
    private String DRIVER_URL;
    private String USER;
    private String PASSWORD;

    public Connexion() {
        Properties propiedades = new Properties();
        try {
            InputStream fis = getClass().getResourceAsStream("/config.properties");
            propiedades.load(fis);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Connexion.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Connexion.class.getName()).log(Level.SEVERE, null, ex);
        }

        DRIVER_CLASS_NAME = propiedades.getProperty("Driver_Name");
        DRIVER_URL = propiedades.getProperty("driver_url");
        USER = propiedades.getProperty("user");
        PASSWORD = propiedades.getProperty("passwd");
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(DRIVER_URL, USER, PASSWORD);
    }

    public Connection getConnectionAdmin() throws SQLException {
        return DriverManager.getConnection(DRIVER_URL, USER, PASSWORD);
    }
}
