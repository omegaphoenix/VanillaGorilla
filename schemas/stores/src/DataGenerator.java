
import java.io.*;
import java.util.*;


public class DataGenerator {

  private static Random random = new Random(6543210L);


  private static class NameData {
    public String name;

    public float freq;

    public int count;

    public int rank;


    public NameData(String name, float freq, int count, int rank) {
      this.name = name;
      this.freq = freq;
      this.count = count;
      this.rank = rank;
    }
  }


  private static class CityData {

    public int id;

    public String cityName;

    public String stateName;

    public int count;

    public int rank;


    public CityData(String cityName, String stateName, int count, int rank) {
      this.cityName = cityName;
      this.stateName = stateName;
      this.count = count;
      this.rank = rank;
    }
  }


  private static class StoreData {

    public int id;

    public CityData storeCity;

    public int propertyCosts;


    public StoreData(int id, CityData storeCity, int propertyCosts) {
      this.id = id;
      this.storeCity = storeCity;
      this.propertyCosts = propertyCosts;
    }
  }




  private static List<CityData> readCityFile(String filename, int skipLines)
    throws IOException {

    ArrayList<CityData> values = new ArrayList<CityData>();

    BufferedReader bufReader = new BufferedReader(new FileReader(filename));

    while (true) {
      // Read the next line of data from the input files.
      String line = bufReader.readLine();
      if (line == null)
        break;

      if (skipLines > 0) {
        skipLines--;
        continue;
      }

      String[] parts = line.split("\t");

      int rank = Integer.parseInt(parts[0].trim());

      String cityName = parts[1].trim();
      String stateName = parts[2].trim();

      int count = Integer.parseInt(parts[3].trim().replaceAll(",", ""));

      values.add(new CityData(cityName, stateName, count, rank));
    }

    return values;
  }


  private static List<NameData> readFirstNameFile(String filename, int skipLines)
    throws IOException {

    ArrayList<NameData> values = new ArrayList<NameData>();

    BufferedReader bufReader = new BufferedReader(new FileReader(filename));

    while (true) {
      // Read the next line of data from the input files.
      String line = bufReader.readLine();
      if (line == null)
        break;

      if (skipLines > 0) {
        skipLines--;
        continue;
      }

      String[] parts = line.split("\t");

      String name = parts[0].trim();
      float freq = Float.parseFloat(parts[1].trim());
      int count = Integer.parseInt(parts[2].trim().replaceAll(",", ""));
      int rank = Integer.parseInt(parts[3].trim());

      values.add(new NameData(name, freq, count, rank));
    }

    return values;
  }


  private static List<NameData> readLastNameFile(String filename, int skipLines)
    throws IOException {

    ArrayList<NameData> values = new ArrayList<NameData>();

    BufferedReader bufReader = new BufferedReader(new FileReader(filename));

    while (true) {
      // Read the next line of data from the input files.
      String line = bufReader.readLine();
      if (line == null)
        break;

      if (skipLines > 0) {
        skipLines--;
        continue;
      }

      String[] parts = line.split("\t");

      String name = parts[0].trim();
      int count = Integer.parseInt(parts[1].trim().replaceAll(",", ""));
      float freq = Float.parseFloat(parts[2].trim());
      int rank = Integer.parseInt(parts[3].trim());

      values.add(new NameData(name, freq, count, rank));
    }

    return values;
  }


  public static void main(String[] args) {

    int numStores = 2000;

    int minEmployeesPerStore = 5;     // 40
    int maxEmployeesPerStore = 20;    // 300


    String dataPath = ".." + File.separator + "res" + File.separator;

    List<CityData> cities = null;

    List<NameData> maleFirstNames = null;
    List<NameData> femaleFirstNames = null;
    List<NameData> lastNames = null;

    try {
      cities = readCityFile(dataPath + "cities.txt", 0);

      maleFirstNames = readFirstNameFile(dataPath + "male-names.txt", 2);
      femaleFirstNames = readFirstNameFile(dataPath + "female-names.txt", 2);

      lastNames = readLastNameFile(dataPath + "surnames-5000.txt", 2);
    }
    catch (Exception e) {
      e.printStackTrace();
      System.exit(2);
    }

    System.err.println("Loaded " + cities.size() + " cities.");
    System.err.println("Loaded " + maleFirstNames.size() + " male first names.");
    System.err.println("Loaded " + femaleFirstNames.size() + " female first names.");
    System.err.println("Loaded " + lastNames.size() + " surnames.");

    int id;

    TreeSet<String> states = new TreeSet<String>();
    for (CityData city : cities)
      states.add(city.stateName);

    System.err.println("Found " + states.size() + " states.");

    HashMap<String, Integer> stateIDs = new HashMap<String, Integer>();

    id = 1;
    for (String stateName : states) {
      stateIDs.put(stateName, id);
      System.out.println("INSERT INTO states VALUES (" + id + ", '" +
        stateName + "');");

      id++;
    }

    System.err.println("Generated " + (id - 1) + " states.");

    System.out.println();

    id = 1;
    for (CityData city : cities) {
      city.id = id;
      System.out.println("INSERT INTO cities VALUES (" + id + ", '" +
        city.cityName + "', " + city.count + ", " +
        stateIDs.get(city.stateName) + ");");

      id++;
    }

    System.err.println("Generated " + (id - 1) + " cities.");

    System.out.println();

    ArrayList<StoreData> stores = new ArrayList<StoreData>();

    id = 1;
    for (int i = 0; i < numStores; i++) {
      StoreData store = new StoreData(id,
        cities.get(random.nextInt(cities.size())), 1000 * random.nextInt(1000));

      stores.add(store);

      System.out.println("INSERT INTO stores VALUES (" + id + ", " +
        store.storeCity.id + ", " + store.propertyCosts + ");");

      id++;
    }

    System.err.println("Generated " + (id - 1) + " stores.");

    System.out.println();

    id = 1;

    for (StoreData store : stores) {

      int empsAtStore = minEmployeesPerStore +
        random.nextInt(maxEmployeesPerStore - minEmployeesPerStore + 1);

      ArrayList<Integer> storeEmployeeIDs = new ArrayList<Integer>();

      for (int emp = 0; emp < empsAtStore; emp++) {

        String firstName, lastName;

        if (random.nextBoolean())
          firstName = maleFirstNames.get(random.nextInt(maleFirstNames.size())).name;
        else
          firstName = femaleFirstNames.get(random.nextInt(femaleFirstNames.size())).name;

        lastName = lastNames.get(random.nextInt(lastNames.size())).name;

        CityData workCity = cities.get(random.nextInt(cities.size()));
        CityData homeCity = findCityInSameState(workCity, cities);

        int salary = (35 + random.nextInt(80 - 35 + 1)) * 1000;

        Integer managerID = null;
        if (storeEmployeeIDs.size() > 0 && random.nextFloat() <= 0.9f)
          managerID = storeEmployeeIDs.get(random.nextInt(storeEmployeeIDs.size()));

        System.out.println("INSERT INTO employees VALUES (" + id + ", '" +
          lastName + "', '" + firstName + "', " + homeCity.id + ", " +
          workCity.id + ", " + salary + ", " +
          (managerID == null ? "NULL" : managerID.toString()) + ");");

        storeEmployeeIDs.add(id);

        id++;
      }
    }

    System.err.println("Generated " + (id - 1) + " employees.");

  }


  private static CityData findCityInSameState(CityData c, List<CityData> cities) {
    CityData result;

    do {
      result = cities.get(random.nextInt(cities.size()));
    }
    while (!c.stateName.equals(result.stateName));

    return result;
  }
}
