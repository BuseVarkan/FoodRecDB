package ceng.ceng351.foodrecdb;

import java.sql.*;
import java.util.ArrayList;

public class FOODRECDB implements IFOODRECDB{

    private static String user = "e2449007"; // TODO: Your userName
    private static String password = "CnTxl21vCHKK4Huk"; //  TODO: Your password
    private static String host = "momcorp.ceng.metu.edu.tr"; // host name
    private static String database = "db2449007"; // TODO: Your database name
    private static int port = 8080; // port

    private static Connection connection = null;
    private Connection con;

    @Override
    public void initialize() {
        String url = "jdbc:mysql://" + this.host + ":" + this.port + "/" + this.database;

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            this.con =  DriverManager.getConnection(url, this.user, this.password);
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
//
    @Override
    public int dropTables() {
        int numberofTablesDropped = 0;

        String queryDropMenuItemsTable = "drop table if exists MenuItems";

        String queryDropIngredientsTable = "drop table if exists Ingredients";

        String queryDropIncludesTable = "drop table if exists Includes";

        String queryDropRatingsTable = "drop table if exists Ratings";

        String queryDropDietaryCategoriesTable = "drop table if exists DietaryCategories";

        try {
            Statement statement = this.con.createStatement();

            statement.executeUpdate(queryDropIncludesTable);
            numberofTablesDropped++;

            statement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            Statement statement = this.con.createStatement();

            statement.executeUpdate(queryDropRatingsTable);
            numberofTablesDropped++;

            statement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            Statement statement = this.con.createStatement();

            statement.executeUpdate(queryDropDietaryCategoriesTable);
            numberofTablesDropped++;

            statement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            Statement statement = this.con.createStatement();

            statement.executeUpdate(queryDropMenuItemsTable);
            numberofTablesDropped++;

            statement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            Statement statement = this.con.createStatement();

            statement.executeUpdate(queryDropIngredientsTable);
            numberofTablesDropped++;

            statement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return numberofTablesDropped;
    }

    @Override
    public int createTables() {

        int numberofTablesInserted = 0;

        // MenuItems(itemID:int, itemName:varchar(40), cuisine:varchar(20), price:int)
        String queryCreateMenuItemsTable = "create table if not exists MenuItems(" +
                "itemID int," +
                "itemName varchar(40)," +
                "cuisine varchar(20)," +
                "price int," +
                "primary key (itemID))";
        try {
            Statement statement = this.con.createStatement();
            statement.executeUpdate(queryCreateMenuItemsTable);
            numberofTablesInserted++;

            //close
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }


        // Ingredients(ingredientID:int, ingredientName:varchar(40))
        String queryCreateIngredientsTable = "create table if not exists Ingredients(" +
                "ingredientID int,"+
                "ingredientName varchar(40),"+
                "primary key (ingredientID))";
        try {
            Statement statement = this.con.createStatement();
            statement.executeUpdate(queryCreateIngredientsTable);
            numberofTablesInserted++;

            //close
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Includes(itemID:int, ingredientID:int)
        String queryCreateIncludesTable = "create table if not exists Includes(" +
                "itemID int," +
                "ingredientID int,"+
                "primary key (itemID, ingredientID),"+
                "foreign key (itemID) references MenuItems(itemID) on delete cascade on update cascade,"+
                "foreign key (ingredientID) references Ingredients(ingredientID) on delete cascade on update cascade)";

        try {
            Statement statement = this.con.createStatement();
            statement.executeUpdate(queryCreateIncludesTable);
            numberofTablesInserted++;

            //close
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //Ratings(ratingID:int, itemID:int, rating:int, ratingDate:date)
        String queryCreateRatingsTable = "create table if not exists Ratings(" +
                "ratingID int," +
                "itemID int,"+
                "rating int,"+
                "ratingDate date,"+
                "primary key (ratingID),"+
                "foreign key (itemID) references MenuItems(itemID) on delete cascade on update cascade)";

        try {
            Statement statement = this.con.createStatement();
            statement.executeUpdate(queryCreateRatingsTable);
            numberofTablesInserted++;

            //close
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //DietaryCategories(ingredientID:int, dietaryCategory:varchar(20))
        String queryCreateDietaryCategoriesTable = "create table if not exists DietaryCategories(" +
                "ingredientID int," +
                "dietaryCategory varchar(20),"+
                "primary key (ingredientID, dietaryCategory),"+
                "foreign key (ingredientID) references Ingredients(ingredientID))";

        try {
            Statement statement = this.con.createStatement();
            statement.executeUpdate(queryCreateDietaryCategoriesTable);
            numberofTablesInserted++;

            //close
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return numberofTablesInserted;
    }

    @Override
    public int insertMenuItems(MenuItem[] items) {
        int numberofRowsInserted = 0;
        //MenuItems(itemID:int, itemName:varchar(40), cuisine:varchar(20), price:int)
        for (MenuItem item : items) {
            try {
                PreparedStatement stmt = this.con.prepareStatement("insert into MenuItems(" +
                        "itemID, " +
                        "itemName, " +
                        "cuisine, " +
                        "price) values (?,?,?,?);");
                stmt.setInt(1, item.getItemID());
                stmt.setString(2, item.getItemName());
                stmt.setString(3, item.getCuisine());
                stmt.setInt(4, item.getPrice());

                stmt.executeUpdate();

                //Close
                stmt.close();
                numberofRowsInserted++;

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return numberofRowsInserted;
    }

    @Override
    public int insertIngredients(Ingredient[] ingredients) {
        int numberofRowsInserted = 0;
        //Ingredients(ingredientID:int, ingredientName:varchar(40))
        for (Ingredient ingredient : ingredients) {
            try {
                PreparedStatement stmt = this.con.prepareStatement("insert into Ingredients(" +
                        "ingredientID, " +
                        "ingredientName) values (?,?);");
                stmt.setInt(1, ingredient.getIngredientID());
                stmt.setString(2, ingredient.getIngredientName());
                stmt.executeUpdate();

                //Close
                stmt.close();
                numberofRowsInserted++;

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return numberofRowsInserted;
    }

    @Override
    public int insertIncludes(Includes[] includes) {
        int numberofRowsInserted = 0;
        // Includes(itemID:int, ingredientID:int)
        for (Includes include : includes) {
            try {
                PreparedStatement stmt = this.con.prepareStatement("insert into Includes(" +
                        "itemID, " +
                        "ingredientID) values (?,?);");
                stmt.setInt(1, include.getItemID());
                stmt.setInt(2, include.getIngredientID());
                stmt.executeUpdate();

                //Close
                stmt.close();
                numberofRowsInserted++;

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return numberofRowsInserted;
    }

    @Override
    public int insertRatings(Rating[] ratings) {
        int numberofRowsInserted = 0;
        // Ratings(ratingID:int, itemID:int, rating:int, ratingDate:date)
        for (Rating rating : ratings) {
            try {
                PreparedStatement stmt = this.con.prepareStatement("insert into Ratings(" +
                        "ratingID, " +
                        "itemID,"+
                        "rating,"+
                        "ratingDate) values (?,?,?,?);");
                stmt.setInt(1, rating.getRatingID());
                stmt.setInt(2, rating.getItemID());
                stmt.setInt(3, rating.getRating());
                stmt.setString(4, rating.getRatingDate());
                stmt.executeUpdate();

                //Close
                stmt.close();
                numberofRowsInserted++;

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return numberofRowsInserted;
    }

    @Override
    public int insertDietaryCategories(DietaryCategory[] categories) {
        int numberofRowsInserted = 0;
        // DietaryCategories(ingredientID:int, dietaryCategory:varchar(20))
        for (DietaryCategory category : categories) {
            try {
                PreparedStatement stmt = this.con.prepareStatement("insert into DietaryCategories(" +
                        "ingredientID, " +
                        "dietaryCategory) values (?,?);");
                stmt.setInt(1, category.getIngredientID());
                stmt.setString(2, category.getDietaryCategory());
                stmt.executeUpdate();

                //Close
                stmt.close();
                numberofRowsInserted++;

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return numberofRowsInserted;
    }


    @Override
    public MenuItem[] getMenuItemsWithGivenIngredient(String name) {
        ResultSet rs;
        ArrayList<MenuItem> reslist = new ArrayList<>();

        String query = "SELECT DISTINCT M.itemID, M.itemName, M.cuisine, M.price\n" +
                "FROM MenuItems M, Ingredients Ig, Includes Ic\n" +
                "WHERE M.itemID = Ic.itemID AND Ig.ingredientID = Ic.ingredientID AND Ig.ingredientName = "+"'"+name+"'"+"\n" +
                "ORDER BY M.itemID ASC;" ;
        try {
            Statement st = this.con.createStatement();
            rs = st.executeQuery(query);

            while (rs.next()) {

                int itemID = rs.getInt("itemID");
                String itemName = rs.getString("itemName");
                String cuisine = rs.getString("cuisine");
                int price = rs.getInt("price");

                MenuItem obj = new MenuItem(itemID, itemName, cuisine, price);
                reslist.add(obj);
            }

            //Close
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        MenuItem[] resarray = new MenuItem[reslist.size()];

        return reslist.toArray(resarray);
    }

    @Override
    public MenuItem[] getMenuItemsWithoutAnyIngredient() {
        ResultSet rs;
        ArrayList<MenuItem> reslist = new ArrayList<>();

        String query = "SELECT DISTINCT M.itemID, M.itemName, M.cuisine, M.price\n" +
                "FROM MenuItems M, Ingredients Ig, Includes Ic\n" +
                "WHERE M.itemID NOT IN (SELECT M1.itemID FROM MenuItems M1, Ingredients Ig1, Includes Ic1" +
                                        " WHERE M1.itemID = Ic1.itemID AND Ig1.ingredientID = Ic1.ingredientID)\n" +
                "ORDER BY M.itemID ASC;" ;
        try {
            Statement st = this.con.createStatement();
            rs = st.executeQuery(query);

            while (rs.next()) {

                int itemID = rs.getInt("itemID");
                String itemName = rs.getString("itemName");
                String cuisine = rs.getString("cuisine");
                int price = rs.getInt("price");

                MenuItem obj = new MenuItem(itemID, itemName, cuisine, price);
                reslist.add(obj);
            }

            //Close
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        MenuItem[] resarray = new MenuItem[reslist.size()];

        return reslist.toArray(resarray);
    }

    @Override
    public Ingredient[] getNotIncludedIngredients() {
        ResultSet rs;
        ArrayList<Ingredient> reslist = new ArrayList<>();

        String query = "SELECT DISTINCT Ig.ingredientID, Ig.ingredientName\n" +
                "FROM MenuItems M, Ingredients Ig, Includes Ic\n" +
                "WHERE Ig.ingredientID NOT IN (SELECT Ig1.ingredientID FROM MenuItems M1, Ingredients Ig1, Includes Ic1" +
                " WHERE M1.itemID = Ic1.itemID AND Ig1.ingredientID = Ic1.ingredientID)\n" +
                "ORDER BY Ig.ingredientID ASC;" ;
        try {
            Statement st = this.con.createStatement();
            rs = st.executeQuery(query);

            while (rs.next()) {

                int ingredientID = rs.getInt("ingredientID");
                String ingredientName = rs.getString("ingredientName");

                Ingredient obj = new Ingredient(ingredientID, ingredientName);
                reslist.add(obj);
            }

            //Close
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        Ingredient[] resarray = new Ingredient[reslist.size()];

        return reslist.toArray(resarray);
    }

    @Override
    public MenuItem getMenuItemWithMostIngredients() {
        ResultSet rs;
        MenuItem obj = null;

        String query =  "select distinct M.itemID, M.itemName, M.cuisine, M.price \n" +
                "from MenuItems as M, Includes Inc \n" +
                "where M.itemID = Inc.itemID \n" +
                "group by M.itemID, M.itemName, M.cuisine, M.price \n" +
                "having count(*) = (select MAX(temp.counts) \n" +
                "from (SELECT M1.itemID, COUNT(*) as counts FROM MenuItems M1, Includes Inc1\n" +
                "WHERE M1.itemID = Inc1.itemID\n" +
                "GROUP BY M1.itemID ) \n" +
                "as temp);";
        try {
            Statement st = this.con.createStatement();
            rs = st.executeQuery(query);

            if (rs.next()) {
                int itemID = rs.getInt("itemID");
                String itemName = rs.getString("itemName");
                String cuisine = rs.getString("cuisine");
                int price = rs.getInt("price");
                obj = new MenuItem(itemID, itemName, cuisine, price); }

            //Close
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return obj;
    }

    @Override
    public QueryResult.MenuItemAverageRatingResult[] getMenuItemsWithAvgRatings() {
        ResultSet rs;
        ArrayList<QueryResult.MenuItemAverageRatingResult> reslist = new ArrayList<>();

        String query = "SELECT DISTINCT M.itemID, M.itemName, avg(R.rating) as avgRating \n" +
                "FROM MenuItems M NATURAL LEFT OUTER JOIN Ratings R\n" +
                "Group by M.itemID \n " +
                "ORDER BY avgRating DESC;";
        try {
            Statement st = this.con.createStatement();
            rs = st.executeQuery(query);

            while (rs.next()) {
                String itemID = rs.getString("itemID");
                String itemName = rs.getString("itemName");
                String avgRating = rs.getString("avgRating");

                QueryResult.MenuItemAverageRatingResult obj = new QueryResult.MenuItemAverageRatingResult(itemID, itemName, avgRating);
                reslist.add(obj);
            }

            //Close
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        QueryResult.MenuItemAverageRatingResult[] resarray = new QueryResult.MenuItemAverageRatingResult[reslist.size()];

        return reslist.toArray(resarray);
    }

    @Override
    public MenuItem[] getMenuItemsForDietaryCategory(String category) {
        ResultSet rs;
        ArrayList<MenuItem> reslist = new ArrayList<>();

        String query = "select MI.itemID,MI.itemName,MI.cuisine,MI.price\n" +
                "from MenuItems MI, Ingredients I2, Includes IC2\n" +
                "where IC2.itemID = MI.itemID and I2.ingredientID = IC2.ingredientID\n" +
                "group by MI.itemId\n" +
                "having COUNT(*) = (SELECT COUNT(*)\n" +
                "from Ingredients I, Includes IC, DietaryCategories DC\n" +
                "where IC.itemID = MI.itemID and I.ingredientID = IC.ingredientID and DC.ingredientId = I.ingredientID\n" +
                "and DC.dietaryCategory = " + "'" + category + "')" +
                " order by MI.itemID asc;";

        try {
            Statement st = this.con.createStatement();
            rs = st.executeQuery(query);

            while (rs.next()) {

                int itemID = rs.getInt("itemID");
                String itemName = rs.getString("itemName");
                String cuisine = rs.getString("cuisine");
                int price = rs.getInt("price");

                MenuItem obj = new MenuItem(itemID, itemName, cuisine, price);
                reslist.add(obj);
            }

            //Close
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        MenuItem[] resarray = new MenuItem[reslist.size()];

        return reslist.toArray(resarray);
    }

    @Override
    public Ingredient getMostUsedIngredient() {
        ResultSet rs;
        Ingredient obj = null;

        String query =  "select distinct Ing.ingredientID, Ing.ingredientName \n" +
                "from MenuItems as M, Includes Inc, Ingredients Ing \n" +
                "where M.itemID = Inc.itemID and Inc.ingredientID = Ing.ingredientID \n" +
                "group by Ing.ingredientID \n" +
                "having count(*) = (select MAX(temp.counts) \n" +
                "from (SELECT Ing.ingredientID, COUNT(*) as counts FROM MenuItems M1, Includes Inc1, Ingredients Ing1\n" +
                "WHERE M1.itemID = Inc1.itemID and Inc1.ingredientID = Ing1.ingredientID\n" +
                "GROUP BY Ing1.ingredientID ) \n" +
                "as temp);";
        try {
            Statement st = this.con.createStatement();
            rs = st.executeQuery(query);

            if (rs.next()) {
                int ingredientID = rs.getInt("ingredientID");
                String ingredientName = rs.getString("ingredientName");
                obj = new Ingredient(ingredientID, ingredientName); }

            //Close
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return obj;
    }

    @Override
    public QueryResult.CuisineWithAverageResult[] getCuisinesWithAvgRating() {
        ResultSet rs;
        ArrayList<QueryResult.CuisineWithAverageResult> reslist = new ArrayList<>();

        String query = "SELECT DISTINCT M.cuisine, avg(R.rating) as averageRating \n" +
                "FROM MenuItems M NATURAL LEFT OUTER JOIN Ratings R\n" +
                "Group by M.cuisine \n " +
                "ORDER BY averageRating DESC;";
        try {
            Statement st = this.con.createStatement();
            rs = st.executeQuery(query);

            while (rs.next()) {
                String cuisine = rs.getString("cuisine");
                String averageRating = rs.getString("averageRating");

                QueryResult.CuisineWithAverageResult obj = new QueryResult.CuisineWithAverageResult(cuisine,averageRating);
                reslist.add(obj);
            }

            //Close
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        QueryResult.CuisineWithAverageResult[] resarray = new QueryResult.CuisineWithAverageResult[reslist.size()];

        return reslist.toArray(resarray);
    }

    @Override
    public QueryResult.CuisineWithAverageResult[] getCuisinesWithAvgIngredientCount() {
        ResultSet rs;
        ArrayList<QueryResult.CuisineWithAverageResult> reslist = new ArrayList<>();

        String query = "SELECT temp.cuisine, Avg(temp.cnt) as averageCount\n" +
                "FROM (select M.cuisine, M.itemId, count(inc.itemID) as cnt " +
                "from MenuItems M NATURAL LEFT OUTER JOIN Includes inc " +
                "group by M.cuisine, M.itemID) as temp \n" +
                "Group by temp.cuisine\n" +
                "order by averageCount desc;";
        try {
            Statement st = this.con.createStatement();
            rs = st.executeQuery(query);

            while (rs.next()) {
                String cuisine = rs.getString("cuisine");
                String averageCount = rs.getString("averageCount");

                QueryResult.CuisineWithAverageResult obj = new QueryResult.CuisineWithAverageResult(cuisine,averageCount);
                reslist.add(obj);
            }

            //Close
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        QueryResult.CuisineWithAverageResult[] resarray = new QueryResult.CuisineWithAverageResult[reslist.size()];

        return reslist.toArray(resarray);
    }

    @Override
    public int increasePrice(String ingredientName, String increaseAmount) {
        int num_of_affected_rows = 0;
        try {

            PreparedStatement stmt=this.con.prepareStatement("UPDATE MenuItems M, Includes Inc, Ingredients Ing\n" +
                    "SET    M.price = M.price + " +increaseAmount+" \n" +
                    "WHERE  M.itemID = Inc.itemID and Inc.ingredientID = Ing.ingredientID and Ing.ingredientName = "+"'" + ingredientName+"'");

            num_of_affected_rows=stmt.executeUpdate();

            //close
            stmt.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return num_of_affected_rows;

    }

    @Override
    public Rating[] deleteOlderRatings(String date) {
        ResultSet rs;
        ArrayList<Rating> reslist = new ArrayList<>();

        try {

            PreparedStatement stmt=this.con.prepareStatement("SELECT DISTINCT  R.ratingID, R.itemID, R.rating, R.ratingDate\n" +
                    "FROM Ratings R\n" +
                    "WHERE R.ratingDate < "+"'"+date+"'\n" +
                    "order by R.ratingID asc");
            rs = stmt.executeQuery();

            while(rs.next()){

            int ratingID = rs.getInt("ratingID");
            int itemID = rs.getInt("itemID");
            int rating = rs.getInt("rating");
            String ratingDate = rs.getString("ratingDate");

            Rating obj = new Rating(ratingID, itemID, rating, ratingDate);
            reslist.add(obj);
            }

            try {
                PreparedStatement stmt2=this.con.prepareStatement("delete from Ratings R where R.ratingDate < "+"'"+date+"'");

                stmt2.executeUpdate();

                //close
                stmt2.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }

            //Close
            stmt.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        Rating[] resarray = new Rating[reslist.size()];
        return reslist.toArray(resarray);
    }
}