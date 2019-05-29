/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nicolau.termoboy.javacronpasardatossql;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Cho_S
 */
public class Controlador {
    public static void main(String[] args) {
        try {
            CRONPasarDatosSQL nPasarDatosSQL =  new CRONPasarDatosSQL();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Controlador.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException | InterruptedException | ParseException | IOException ex) {
            Logger.getLogger(Controlador.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
