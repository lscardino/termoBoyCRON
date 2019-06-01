/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nicolau.termoboy.javacronpasardatossql;

import com.nicolau.termoboy.javacronpasardatossql.modelo.Connexion;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.Query;
import com.google.firebase.database.ValueEventListener;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Lorenzo
 */
public class CRONPasarDatosSQL {

    final int CANTIDAD_LINEA;
    private FirebaseDatabase database;
    private DatabaseReference ref;
    private CountDownLatch latch;
    private Connection conn;

    private SimpleDateFormat parser;

    public CRONPasarDatosSQL(int cantidadLinea) throws FileNotFoundException, IOException, SQLException, InterruptedException, ParseException {
        CANTIDAD_LINEA = cantidadLinea;

        InputStream serviceAccount = getClass().getResourceAsStream("/termomovidas-firebase-adminsdk-qgjn6-378a7de574.json");
        System.out.println(serviceAccount.read());
        serviceAccount = getClass().getResourceAsStream("/termomovidas-firebase-adminsdk-qgjn6-378a7de574.json");
        FirebaseOptions options = new FirebaseOptions.Builder()
                .setCredentials(GoogleCredentials.fromStream(serviceAccount))
                .setDatabaseUrl("https://termomovidas.firebaseio.com/")
                .build();

        FirebaseApp.initializeApp(options);
        inicilizaVariables();
        System.out.println("INFO Actualizará las últimas fechas menos " + CANTIDAD_LINEA);
        leerBDD();

        System.out.println("   BORRAR DATOS");
        CRONBorrarUltimaSemana borrarUltimaSemana = new CRONBorrarUltimaSemana(CANTIDAD_LINEA);
        System.out.println("Calse ejecutada con Exito.");
    }

    /**
     * @see Método que lee de Firebase y escribe en SQL
     * @throws InterruptedException
     */
    private void leerBDD() throws InterruptedException {

        latch = new CountDownLatch(1);

        System.out.print("Usuario - ");
        database.getReference("Usuario").addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot ds) {
                for (DataSnapshot usuariosDatos : ds.getChildren()) {
                    String idUsuario = usuariosDatos.getKey().toString();
                    
                    System.out.print(".");
                    if (miraSiUsuarioRepetido(idUsuario)) {
                        //System.out.print( idUsuario + " |");
                        guardarSQL_Usuario(idUsuario, Integer.parseInt(usuariosDatos.child("Edad").getValue().toString()), usuariosDatos.child("Sexo").getValue().toString());
                    }
                }
                latch.countDown();
            }

            @Override
            public void onCancelled(DatabaseError de) {
                latch.countDown();
            }
        }
        );
        try {
            latch.await();
        } catch (InterruptedException ex) {
            Logger.getLogger(CRONPasarDatosSQL.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            System.out.println("");
        }

        ref = database.getReference("Dia");
        latch = new CountDownLatch(1);
        System.out.println("Dia - ");
        ref.addListenerForSingleValueEvent(new ValueEventListener() {

            //TRANSPORTES
            private long totalEntradas;
            private Query query;

            @Override
            public void onDataChange(DataSnapshot ds) {
                final int cantidadDatos = (int) ds.getChildrenCount();

                if (cantidadDatos > CANTIDAD_LINEA) {
                    query = ref.orderByKey().limitToFirst(cantidadDatos - CANTIDAD_LINEA);
                } else {
                    System.out.println("\nERROR: No hay datos que concuerden con la busqueda especificada.");
                }
                query.addListenerForSingleValueEvent(new ValueEventListener() {

                    private String diaEntrada;

                    @Override
                    public void onDataChange(DataSnapshot ds) {
                        for (DataSnapshot entrada : ds.getChildren()) {

                            diaEntrada = entrada.getKey();
                            System.out.println("\nINFO Día subir " + diaEntrada);
                            //CreaFecha
                            miraSiFechaRepetida(diaEntrada);

                            System.out.print("    INFO Hora EntradaS -> ");
                            for (DataSnapshot horas : entrada.child("Hora").getChildren()) {
                                String horaEntrada = horas.getKey();
                                
                                System.out.print(".");
                                if (!mirarSiHoraRepetida(diaEntrada, horaEntrada)) {
                                    //System.out.print(horaEntrada + "| ");
                                    String humedadEntrada = horas.child("Humedad").getValue().toString();
                                    String temperaturaEntrada = horas.child("Temperatura").getValue().toString();
                                    String lluviaEntrada = horas.child("Lluvia").getValue().toString();
                                    String polvoEntrada = horas.child("Polvo").getValue().toString();
                                    String presionEntrada = horas.child("Presión").getValue().toString();
                                    String velVientoEntrada = horas.child("Velocidad viento").getValue().toString();
                                    String sensacionTEntrada = horas.child("Sensacion").getValue().toString();
                                    String lumensEntrada = horas.child("Lumens").getValue().toString();
                                    guardarSQL_Hora(diaEntrada, horaEntrada, humedadEntrada, temperaturaEntrada, presionEntrada, lluviaEntrada,
                                            velVientoEntrada, polvoEntrada, sensacionTEntrada, lumensEntrada);
                                }
                            }
                            System.out.println("");

                            totalEntradas = entrada.child("Transporte").getChildrenCount();
                            System.out.print("    INFO Transporte " + totalEntradas + " -> ");
                            for (DataSnapshot transportes : entrada.child("Transporte").getChildren()) {
                                String transporte = transportes.getKey();
                                if (!mirarSiTransportesExisten(transporte)) {
                                    guardarSQL_Transporte(transporte);
                                }
                                for (DataSnapshot infoUser : transportes.getChildren()) {
                                    //System.out.print(transporte + "," + usuario + "| ");
                                    System.out.print(".");
                                    
                                    String usuario = infoUser.getKey();
                                    guardarSQL_Usar(usuario, transporte, diaEntrada);
                                }
                            }
                            System.out.println("");

                        }
                        System.out.println("INFO Terminado");
                        latch.countDown();
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
            }
        });
        latch.await();
    }

    private boolean miraSiUsuarioRepetido(String idUsuario) {

        try {
            String queryFecha = "select count(*) from Usuario usu where usu.fk_codigo=(?)";
            PreparedStatement sentenciaP = conn.prepareStatement(queryFecha);
            sentenciaP.setObject(1, idUsuario);
            try (ResultSet rs = sentenciaP.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException ex) {
            Logger.getLogger(CRONPasarDatosSQL.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }

    /**
     * @see Método que mira en SQL si el dato pasado existe o no.
     * @param fecha Dato a verificar si existe no hace nada, pero si no existe
     * la crea en la base de datos
     * @throws ParseException Error al pasar los datos de String a int
     * @throws SQLException Error de la base de datos SQL
     */
    private void miraSiFechaRepetida(String fecha) {
        try {
            String queryFecha = "select count(*) from dia d where d.fk_dia=(?)";
            PreparedStatement sentenciaP = conn.prepareStatement(queryFecha);
            sentenciaP.setObject(1, fecha);
            try (ResultSet rs = sentenciaP.executeQuery()) {
                if (rs.next()) {
                    int resultado = rs.getInt(1);
                    if (resultado == 0) {
                        String insertarFecha = "INSERT INTO dia VALUES(?)";
                        sentenciaP = conn.prepareStatement(insertarFecha);
                        sentenciaP.setObject(1, fecha);
                        sentenciaP.execute();
                    }
                } else {
                    System.out.println("Errror");
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(CRONPasarDatosSQL.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * @see Método para mirar si el transporte existe paso existe en la base de
     * datos, a referencia a la tabla transporte
     * @param transporte Datos pasado para realitzar la consulta
     * @return Devuelve un booleano de si existe o no.
     * @throws ParseException
     * @throws SQLException
     */
    private boolean mirarSiTransportesExisten(String transporte) {
        boolean repe = false;
        try {
            String queryFecha = "select count(*) from Transporte t where t.fk_como=(?)";
            PreparedStatement sentenciaP = conn.prepareStatement(queryFecha);
            sentenciaP.setObject(1, transporte);
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
        } catch (SQLException ex) {
            Logger.getLogger(CRONPasarDatosSQL.class.getName()).log(Level.SEVERE, null, ex);
        }
        return repe;
    }

    /**
     * @see Método para verificar si exite los datos pasado en la base de datos
     * SQL
     * @param dia Dato referencia al dia que está en la columna Dia de datos
     * @param hora Dato referenciado a la hora que está en al columna hora de
     * Datos
     * @return Devuelve un boolean de su exitencia
     * @throws SQLException
     */
    private boolean mirarSiHoraRepetida(String dia, String hora) {
        boolean repetido = false;
        try {

            String queryFecha = "select count(*) from Datos d where d.fk_dia=(?) and d.fk_hora =(?)";
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
        } catch (SQLException ex) {
            Logger.getLogger(CRONPasarDatosSQL.class.getName()).log(Level.SEVERE, null, ex);
        }
        return repetido;
    }

    private void guardarSQL_Usuario(String idUsuario, int edad, String genero) {
        try {
            String insertarFecha = "INSERT INTO usuario VALUES(?,?,?)";
            PreparedStatement sentenciaP = conn.prepareStatement(insertarFecha);
            sentenciaP.setObject(1, idUsuario);
            sentenciaP.setObject(2, edad);
            sentenciaP.setObject(3, genero);
            sentenciaP.execute();
        } catch (SQLException ex) {
        }
    }

    /**
     * @see Método que realitza el almacenaje de los datos en SQL
     * @param dia
     * @param hora
     * @param humedad
     * @param temperatura
     * @param presion
     * @param mmlluvia
     * @param kmhViento
     * @param nivelPolvo
     * @param sensacionT
     * @throws SQLException
     * @throws ParseException
     */
    private void guardarSQL_Hora(String dia, String hora, String humedad,
            String temperatura, String presion, String mmlluvia,
            String kmhViento, String nivelPolvo, String sensacionT, String lumens
    ) {
        try {
            //System.out.println("\n\nFecha que vamos a insertar " + dia + " Hora que vamso a insertar: " + hora);

            String insertarlosDatos = "INSERT INTO Datos VALUES (?,?,?,?,?,?,?,?,?,?)";
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
            sentenciaP.setFloat(10, Float.parseFloat(lumens));

            sentenciaP.execute();

            //System.out.println("Escrito Todo guay");
        } catch (SQLException ex) {
        } catch (NumberFormatException em) {

        }
    }

    private void guardarSQL_Transporte(String idTransporte) {
        try {
            String query = "insert into Transporte"
                    + " values (?)";
            PreparedStatement sentenciaP = conn.prepareStatement(query);
            sentenciaP.setObject(1, idTransporte);

            sentenciaP.execute();

        } catch (SQLException ex) {
        }
    }

    private void guardarSQL_Usar(String idUser, String vehiculo, String mDia) {
        try {
            String query = "insert into Usar"
                    + " values (?,?,?)";
            PreparedStatement sentenciaP = conn.prepareStatement(query);
            sentenciaP.setObject(1, idUser);
            sentenciaP.setString(2, vehiculo);
            sentenciaP.setObject(3, mDia);

            sentenciaP.execute();
        } catch (SQLException ex) {
        }
    }

    private void inicilizaVariables() throws SQLException {
        database = FirebaseDatabase.getInstance();
        conn = Connexion.getConnection();

        parser = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);

    }
}
