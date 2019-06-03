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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

/**
 *
 * @author Lorenzo
 */
public class CRONBorrarUltimaSemana {

    final int CANTIDAD_LINEA;
    private final FirebaseDatabase database;
    private final DatabaseReference ref;
    private CountDownLatch latch;

    private final ArrayList listaDias;

    public CRONBorrarUltimaSemana(int cantidadlinea) throws FileNotFoundException, IOException, InterruptedException {
        CANTIDAD_LINEA = cantidadlinea;
        /*
        InputStream serviceAccount = getClass().getResourceAsStream("/termomovidas-firebase-adminsdk-qgjn6-378a7de574.json");
        System.out.println(serviceAccount.read());
        serviceAccount = getClass().getResourceAsStream("/termomovidas-firebase-adminsdk-qgjn6-378a7de574.json");

        FirebaseOptions options = new FirebaseOptions.Builder()
                .setCredentials(GoogleCredentials.fromStream(serviceAccount))
                .setDatabaseUrl("https://termomovidas.firebaseio.com/")
                .build();

        FirebaseApp.initializeApp(options);*/

        System.out.println("INFO Borrará las últimas fechas menos " + CANTIDAD_LINEA);
        listaDias = new ArrayList();
        database = FirebaseDatabase.getInstance();
        ref = database.getReference("Dia");

        latch = new CountDownLatch(2);
        obtenerBorrarUsuarios();
        obtenerBorrarDatos();
        latch.await();
    }

    private void obtenerBorrarDatos() throws InterruptedException {

        ref.addListenerForSingleValueEvent(new ValueEventListener() {
            private Query query;

            @Override
            public void onDataChange(DataSnapshot ds) {
                final int cantidadDatos = (int) ds.getChildrenCount();

                if (cantidadDatos > CANTIDAD_LINEA) {
                    query = ref.orderByKey().limitToFirst(cantidadDatos - CANTIDAD_LINEA);
                    query.addListenerForSingleValueEvent(new ValueEventListener() {
                        @Override
                        public void onDataChange(DataSnapshot ds) {
                            System.out.print("    INFO Día Borrar ");
                            for (DataSnapshot entrada : ds.getChildren()) {
                                String diaEntrada = entrada.getKey();
                                System.out.print(diaEntrada + " | ");
                                removeQuery(entrada);
                            }
                            System.out.println("");

                            latch.countDown();
                        }

                        private void removeQuery(DataSnapshot _entrada) {
                            _entrada.getRef().removeValue(new DatabaseReference.CompletionListener() {
                                @Override
                                public void onComplete(DatabaseError de, DatabaseReference dr) {
                                }
                            });
                        }

                        @Override
                        public void onCancelled(DatabaseError de) {
                            latch.countDown();
                        }
                    });
                } else {
                    System.out.println("ERROR: No hay datos que concuerden con la busqueda especificada.");
                    latch.countDown();
                }

            }

            @Override

            public void onCancelled(DatabaseError de) {
            }
        });
    }

    private void obtenerBorrarUsuarios() {

        database.getReference("Usuario").addListenerForSingleValueEvent(new ValueEventListener() {
            final long LIMIT_USER = 100;

            @Override
            public void onDataChange(DataSnapshot ds) {
                long countUsuarios = ds.getChildrenCount();
                int overflowUser = (int) (countUsuarios - LIMIT_USER);
                

                if (overflowUser > 0) {
                    Query cienUsuarios = ds.getRef().limitToFirst(overflowUser);
                    cienUsuarios.addListenerForSingleValueEvent(new ValueEventListener() {
                        @Override
                        public void onDataChange(DataSnapshot ds) {
                            System.out.println("INFO - Limite de usuario superado");
                            System.out.println("    ACCION - Subirá los datos(" + LIMIT_USER + ")");
                            for (DataSnapshot overUser : ds.getChildren()) {
                                removeQuery(overUser);
                            }
                            latch.countDown();
                        }

                        private void removeQuery(DataSnapshot _overUser) {
                            _overUser.getRef().removeValue(new DatabaseReference.CompletionListener() {
                                @Override
                                public void onComplete(DatabaseError de, DatabaseReference dr) {
                                }
                            });
                        }

                        @Override
                        public void onCancelled(DatabaseError de) {
                            latch.countDown();
                        }
                    });
                } else {
                    latch.countDown();
                }
            }

            @Override
            public void onCancelled(DatabaseError de) {
            }
        });
    }
}
