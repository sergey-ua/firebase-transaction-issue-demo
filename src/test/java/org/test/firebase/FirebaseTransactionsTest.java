package org.test.firebase;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.MutableData;
import com.google.firebase.database.Transaction;
import com.google.firebase.database.ValueEventListener;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.test.firebase.config.FirebaseConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

@RunWith(JUnit4.class)
public class FirebaseTransactionsTest {

    private static final String TEST_USER_PATH = "/test/users/u1";

    private FirebaseDatabase firebaseDatabase;

    @Before
    public void tearUp() throws Exception {
        setupFirebaseSdk();
    }

    @After
    public void tearDown() throws Exception {
        firebaseDatabase.getApp().delete();
    }

    @Test
    /**
     * Run this to get initial state
     *  ./mvnw -Dtest=FirebaseTransactionsTest#resetState -DdatabaseUrl="https://your-db-url.firebaseio.com/" -Dcert="/path/to/serviceAccountKey.json" test
     */
    public void resetState() throws Exception {
        removeUserData();
        setupInitialData();

        //lets check initial calls are in place
        assertUserObject();
    }

    @Test
    /**
     * Run this to reproduce issue. This test will fail
     * ./mvnw -Dtest=FirebaseTransactionsTest#subsequentTransactions -DdatabaseUrl="https://your-db-url.firebaseio.com/" -Dcert="/path/to/serviceAccountKey.json" test
     */
    public void subsequentTransactions() throws Exception {
        CountDownLatch latch = new CountDownLatch(3);
        String callUid = UUID.randomUUID().toString();
        //create new call
        firebaseDatabase.getReference(TEST_USER_PATH).child("calls").child("history").child(callUid)
            .runTransaction(new Transaction.Handler() {
                @Override
                public Transaction.Result doTransaction(MutableData currentData) {
                    currentData.child("/").setValue(getCallTemplate("updated"));
                    return Transaction.success(currentData);
                }

                @Override
                public void onComplete(DatabaseError error, boolean committed, DataSnapshot currentData) {
                    latch.countDown();
                }
            });
        //update call we have just created
        firebaseDatabase.getReference(TEST_USER_PATH).child("calls").child("history").child(callUid)
            .runTransaction(new Transaction.Handler() {
                @Override
                public Transaction.Result doTransaction(MutableData currentData) {
                    currentData.child("/").setValue(getCallTemplate("updated"));
                    return Transaction.success(currentData);
                }

                @Override
                public void onComplete(DatabaseError error, boolean committed, DataSnapshot currentData) {
                    latch.countDown();
                }
            });

        //Thread.sleep(5000); <- uncomment this and test will pass

        //update user object
        firebaseDatabase.getReference(TEST_USER_PATH).runTransaction(new Transaction.Handler() {
            @Override
            public Transaction.Result doTransaction(MutableData currentData) {
                currentData.child("lastUpdated").setValue(new Date().getTime());
                return Transaction.success(currentData);
            }

            @Override
            public void onComplete(DatabaseError error, boolean committed, DataSnapshot currentData) {
                latch.countDown();
            }
        });
        latch.await();

        //lets check initial calls are in place
        assertUserObject();
    }


    private void setupFirebaseSdk() throws IOException {
        FirebaseConfig firebaseConfig = FirebaseConfig.getInstance();
        FirebaseOptions options = new FirebaseOptions.Builder()
            .setCredentials(GoogleCredentials.fromStream(firebaseConfig.getCertAsInputStream()))
            .setDatabaseUrl(firebaseConfig.getDatabaseURL())
            .build();
        FirebaseApp app = FirebaseApp.initializeApp(options);
        firebaseDatabase = FirebaseDatabase.getInstance(app);
    }

    private void setupInitialData() throws Exception {
        Map<String, Object> userObject = new HashMap<>();
        userObject.put("name", "test user");
        CountDownLatch countDownLatch = new CountDownLatch(1);
        this.firebaseDatabase.getReference(TEST_USER_PATH).setValue(userObject, (databaseError, databaseReference) -> {
            addCall("c1")
                .thenAccept((r -> addCall("c2")))
                .thenAccept((r -> addCall("c3")))
                .thenAccept((r -> addCall("c4")))
                .thenAccept((r -> addCall("c5")))
                .thenAccept((r -> addCall("c6")))
                .thenAccept((r -> addCall("c7")))
                .thenAccept((r -> addCall("c8")))
                .thenAccept((r -> addCall("c9")))
                .thenAccept((r -> addCall("c10")));
            countDownLatch.countDown();
        });
        countDownLatch.await();
    }

    private CompletableFuture<?> addCall(String callId) {
        Map<String, Object> callTemplate = getCallTemplate(callId);
        CompletableFuture result = new CompletableFuture();
        firebaseDatabase.getReference(TEST_USER_PATH).child("calls").child("history").child(callId)
            .setValue(callTemplate, (databaseError, databaseReference) -> result.complete("completed"));
        return result;
    }

    private Map<String, Object> getCallTemplate(String callId) {
        Map<String, Object> callTemplate = new HashMap<>();
        callTemplate.put("id", callId);
        callTemplate.put("phoneNumber", "9999999");
        return callTemplate;
    }

    private void removeUserData() throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        firebaseDatabase.getReference(TEST_USER_PATH).removeValue((databaseError, databaseReference) -> countDownLatch.countDown());
        countDownLatch.await();
    }

    private void assertUserObject() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        List<DataSnapshot> dataSnapshots = new ArrayList<>();
        firebaseDatabase.getReference(TEST_USER_PATH).addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot snapshot) {
                for (int i=1; i<=10; i++) {
                    dataSnapshots.add(snapshot.child("calls").child("history").child("c" + i));
                }
                latch.countDown();
            }

            @Override
            public void onCancelled(DatabaseError error) {
                latch.countDown();
            }
        });
        latch.await();
        dataSnapshots.forEach((ds) -> Assert.assertTrue(ds.exists()));
    }

}
