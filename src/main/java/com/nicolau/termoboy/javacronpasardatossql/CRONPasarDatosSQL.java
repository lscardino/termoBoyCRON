/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nicolau.termoboy.javacronpasardatossql;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.Query;
import com.google.firebase.database.ValueEventListener;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Lorenzo
 */
public class CRONPasarDatosSQL {

    static FirebaseDatabase database;
    static DatabaseReference ref;
    static DatabaseReference refTransportes;
    static CountDownLatch latch;
    static Connection conn;
    static Query query;

    //TRANSPORTES
    static long totalEntradas;
    static int bici;
    static int coche;
    static int tPublico;
    static int apie;
    static int otros;

    static String diaEntrada;
    static HashMap<String, String> listaTotal;

    static SimpleDateFormat parser;

    public static void main(String[] args) throws FileNotFoundException, IOException, SQLException, InterruptedException, ParseException {
        FileInputStream serviceAccount = new FileInputStream("termomovidas-firebase-adminsdk-qgjn6-378a7de574.json");

        FirebaseOptions options = new FirebaseOptions.Builder()
                .setCredentials(GoogleCredentials.fromStream(serviceAccount))
                .setDatabaseUrl("https://termomovidas.firebaseio.com/")
                .build();

        FirebaseApp.initializeApp(options);
        inicilizaVariables();
        leerBDD();
        latch.await();
        refTransportes = database.getReference("Dia/" + diaEntrada + "/Transporte");
        leerTransportes();
        latch.await();
        System.out.println("Calse ejecutada con Exito.");
    }

    public static void leerBDD() throws InterruptedException {
        latch = new CountDownLatch(1);
        ref.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot ds) {
                int cantidadDatos = (int) ds.getChildrenCount();

                if (cantidadDatos > 1) {
                    query = ref.orderByKey().limitToLast(1);
                } else {
                    System.out.println("ERROR: No hay datos que concuerden con la busqueda especificada.");
                }
                query.addListenerForSingleValueEvent(new ValueEventListener() {
                    @Override
                    public void onDataChange(DataSnapshot ds) {
                        for (DataSnapshot entrada : ds.getChildren()) {
                            try {
                                diaEntrada = entrada.getKey();
                                fechaRepetida(diaEntrada);
                                for (DataSnapshot datos : entrada.getChildren()) {
                                    if (!datos.getKey().equals("Transporte")) {

                                        String horaEntrada = datos.getKey();
                                        if (!horaRepetida(diaEntrada, horaEntrada)) {

                                            try {

                                                String humedadEntrada = datos.child("Humedad").getValue().toString();
                                                String temperaturaEntrada = datos.child("Temperatura").getValue().toString();
                                                String lluviaEntrada = datos.child("Lluvia").getValue().toString();
                                                String polvoEntrada = datos.child("Polvo").getValue().toString();
                                                String presionEntrada = datos.child("Presión").getValue().toString();
                                                String velVientoEntrada = datos.child("Velocidad viento").getValue().toString();
                                                String sensacionTEntrada = datos.child("Sensacion").getValue().toString();

                                                guardarSQL(diaEntrada, horaEntrada, humedadEntrada, temperaturaEntrada, presionEntrada, lluviaEntrada,
                                                        velVientoEntrada, polvoEntrada, sensacionTEntrada);

                                            } catch (ParseException | SQLException ex) {
                                                Logger.getLogger(CRONPasarDatosSQL.class.getName()).log(Level.SEVERE, null, ex);
                                            }
                                        }

                                    }
                                }
                                latch.countDown();

                            } catch (ParseException | SQLException ex) {
                                Logger.getLogger(CRONPasarDatosSQL.class.getName()).log(Level.SEVERE, null, ex);
                            }
                        }
                    }

                    @Override
                    public void onCancelled(DatabaseError de) {
                        System.out.println("ERROR: No hay datos que concuerden con la busqueda especificada.");
                        latch.countDown();
                    }
                });
            }

            @Override
            public void onCancelled(DatabaseError de) {
                System.out.println("ERROR: No hay datos que concuerden con la busqueda especificada.");
                latch.countDown();
            }
        });
        latch.await();
    }

    public static void leerTransportes() throws InterruptedException {
        latch = new CountDownLatch(1);
        listaTotal = new HashMap<>();
        refTransportes.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot ds) {
                totalEntradas = ds.getChildrenCount();
                try {
                    if (!transportesExisten(diaEntrada)) {
                        
                        for (DataSnapshot entrada : ds.getChildren()) {
                            String usuario = entrada.getKey();
                            String transporte = entrada.getValue(String.class);
                            listaTotal.put(usuario, transporte);
                            
                            transformarParaSQL();
                            
                        }
                    }else{
                        System.out.println("Datos ya existentes, no se escribiran"
                                + " datos en la BDD");
                    }
                } catch (ParseException ex) {
                    Logger.getLogger(CRONPasarDatosSQL.class.getName()).log(Level.SEVERE, null, ex);
                } catch (SQLException ex) {
                    Logger.getLogger(CRONPasarDatosSQL.class.getName()).log(Level.SEVERE, null, ex);
                }

                latch.countDown();
            }

            @Override
            public void onCancelled(DatabaseError de) {
                System.out.println("Lectura de datos cancelada");
                latch.countDown();
            }
        });
    }

    public static void fechaRepetida(String fecha) throws ParseException, SQLException {
        String queryFecha = "select count(*) from Dia d where d.dia=(?)";
        PreparedStatement sentenciaP = conn.prepareStatement(queryFecha);
        sentenciaP.setObject(1, fecha);
        try (ResultSet rs = sentenciaP.executeQuery()) {
            if (rs.next()) {
                int resultado = rs.getInt(1);
                if (resultado == 0) {
                    String insertarFecha = "INSERT INTO Dia VALUES(?)";
                    sentenciaP = conn.prepareStatement(insertarFecha);
                    sentenciaP.setObject(1, fecha);
                    sentenciaP.execute();
                }
            } else {
                System.out.println("Errror");
            }
        }
    }
    
        public static boolean transportesExisten(String fecha) throws ParseException, SQLException {
            boolean repe= false;
        String queryFecha = "select count(*) from Transporte t where t.dia=(?)";
        PreparedStatement sentenciaP = conn.prepareStatement(queryFecha);
        sentenciaP.setObject(1, fecha);
        try (ResultSet rs = sentenciaP.executeQuery()) {
            if (rs.next()) {
                int resultado = rs.getInt(1);
                if (resultado > 0) {
                    repe = true;
                }
            } else {
                System.out.println("Errror");
            }
        }
        return repe;
    }

    public static boolean horaRepetida(String dia, String hora) throws SQLException {
        boolean repetido = false;

        String queryFecha = "select count(*) from Datos d where d.dia=(?) and d.hora =(?)";
        PreparedStatement sentenciaP = conn.prepareStatement(queryFecha);
        sentenciaP.setObject(1, dia);
        sentenciaP.setString(2, hora);

        ResultSet rs = sentenciaP.executeQuery();
        if (rs.next()) {
            int resultado = rs.getInt(1);
            if (resultado > 0) {
                repetido = true;
            } else {
                repetido = false;
            }
        } else {
            System.out.println("Ha habido un error tocho!");
        }
        return repetido;
    }

    public static void guardarSQL(String dia, String hora, String humedad,
            String temperatura, String presion, String mmlluvia,
            String kmhViento, String nivelPolvo, String sensacionT
    ) throws SQLException, ParseException {
        //System.out.println("\n\nFecha que vamos a insertar " + dia + " Hora que vamso a insertar: " + hora);

        String insertarlosDatos = "INSERT INTO Datos VALUES (?,?,?,?,?,?,?,?,?)";
        PreparedStatement sentenciaP = conn.prepareStatement(insertarlosDatos);
        sentenciaP.setObject(1, dia);
        sentenciaP.setString(2, hora);
        sentenciaP.setFloat(3, Float.parseFloat(humedad));
        sentenciaP.setFloat(4, Float.parseFloat(temperatura));
        sentenciaP.setFloat(5, Float.parseFloat(presion));
        sentenciaP.setFloat(6, Float.parseFloat(sensacionT));
        sentenciaP.setFloat(7, Float.parseFloat(nivelPolvo));
        sentenciaP.setFloat(8, Float.parseFloat(mmlluvia));
        sentenciaP.setFloat(9, Float.parseFloat(kmhViento));

        sentenciaP.execute();

        //System.out.println("Escrito Todo guay");
    }

    public static void transformarParaSQL() throws SQLException {
        for (Map.Entry pair : listaTotal.entrySet()) {
            switch ((String) pair.getValue()) {
                case "Coche":
                    coche++;
                    break;
                case "Bici":
                    bici++;
                    break;
                case "Apie":
                    apie++;
                    break;
                case "Tpublico":
                    tPublico++;
                    break;
                default:
                    otros++;
                    break;
            }
        }
        enviarSQLT();
    }

    public static void enviarSQLT() throws SQLException {
        String query = "insert into transporte"
                + " values (?,?,?,?,?,?)";
        PreparedStatement sentenciaP = conn.prepareStatement(query);
        sentenciaP.setObject(1, diaEntrada);
        sentenciaP.setInt(2, coche);
        sentenciaP.setInt(3, apie);
        sentenciaP.setInt(4, tPublico);
        sentenciaP.setInt(5, bici);
        sentenciaP.setInt(6, otros);

        sentenciaP.execute();

        /*
        System.out.println("Escrito: ");
        System.out.println("Dia " + diaEntrada);
        System.out.println("Nº gente coche: " + coche);
        System.out.println("Nº gente bici: " + bici);
        System.out.println("Nº gente transportes pub: " + tPublico);
        System.out.println("Nº gente a pie: " + apie);
        System.out.println("Nº gente otros: " + otros);
         */
    }

    public static void inicilizaVariables() throws SQLException {
        database = FirebaseDatabase.getInstance();
        ref = database.getReference("Dia");

        query = ref.orderByKey().limitToFirst(2);
        conn = Connexion.getConnection();

        parser = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);

        bici = 0;
        coche = 0;
        tPublico = 0;
        apie = 0;
        otros = 0;
    }
}