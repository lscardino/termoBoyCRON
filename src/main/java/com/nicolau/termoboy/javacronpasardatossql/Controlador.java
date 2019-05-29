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
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Cho_S
 */
public class Controlador {

    public static void main(String[] args) {
        int CANTIDAD_LINEA = 2;
        try {
            if (args.length == 1) {
                CANTIDAD_LINEA = Integer.parseInt(args[0]);
                System.out.println("INFO Actualizará y Borrará las últimas fechas menos " + CANTIDAD_LINEA);
                System.out.println("Pesioné Enter para continuar");
                new Scanner(System.in).nextLine();
            }
        } catch (NumberFormatException exx) {

        } finally {
            try {
                CRONPasarDatosSQL nPasarDatosSQL = new CRONPasarDatosSQL(CANTIDAD_LINEA);
            } catch (FileNotFoundException ex) {
                Logger.getLogger(Controlador.class.getName()).log(Level.SEVERE, null, ex);
            } catch (SQLException | InterruptedException | ParseException | IOException ex) {
                Logger.getLogger(Controlador.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
