package raven.client.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.List;
import java.util.UUID;

import org.junit.Test;

import raven.client.RavenDBAwareTests;
import raven.client.json.JsonDocument;
import raven.client.json.PutResult;
import raven.client.json.RavenJObject;
import raven.client.json.RavenJValue;

public class ServerClientTest extends RavenDBAwareTests {

  private static class Person {
    private String firstname;
    private String lastname;
    public String getFirstname() {
      return firstname;
    }
    public void setFirstname(String firstname) {
      this.firstname = firstname;
    }
    public String getLastname() {
      return lastname;
    }
    public void setLastname(String lastname) {
      this.lastname = lastname;
    }

  }


  @Test
  public void testDatabaseChanges() throws Exception {
    ServerClient client = new ServerClient(DEFAULT_SERVER_URL);
    IDatabaseCommands systemDatabaseClient = client.forSystemDatabase();
    assertSame(systemDatabaseClient, client);

    createDb("db1");

    List<String> databaseNames = systemDatabaseClient.getDatabaseNames(20, 0);
    System.out.println(databaseNames);

    IDatabaseCommands db1Commands = client.forDatabase("db1");
    Person person1 = new Person();
    person1.setFirstname("John");
    person1.setLastname("Smith");
    PutResult putResult = db1Commands.put("users/marcin", null, RavenJObject.fromObject(person1), null);
    assertNotNull(putResult.getEtag());

    assertNull("Object was created in different db!", client.get("users/marcin"));
    JsonDocument jsonDocument = db1Commands.get("users/marcin");
    assertNotNull(jsonDocument);

    assertEquals(putResult.getEtag(), jsonDocument.getEtag());
    assertEquals(new RavenJValue("John"), jsonDocument.getDataAsJson().get("firstname"));

    Person person2 = new Person();
    person2.setFirstname("Albert");
    person2.setLastname("Einstein");
    PutResult albertPutResult = db1Commands.put("users/albert", null, RavenJObject.fromObject(person2), null);

    List<JsonDocument> jsonDocuments = db1Commands.startsWith("users", "", 0, 10, false);
    assertEquals(2, jsonDocuments.size());


    List<JsonDocument> documents = db1Commands.getDocuments(0, 20, false);
    assertEquals(2, documents.size());
    //TODO: test for all fields!

    db1Commands.delete("users/albert", albertPutResult.getEtag());
    db1Commands.delete("users/albert", UUID.randomUUID());

    assertEquals(1,  db1Commands.getDocuments(0, 20, false).size());

    deleteDb("db1");
  }
}
