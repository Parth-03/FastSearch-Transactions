/* Parth Goel, Humad Syed, Vincent Tran */import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.text.DecimalFormat;
import java.text.NumberFormat;

import java.io.FileInputStream;

/**
 * Runs queries against a back-end database
 */
public class Query {
    private static Properties configProps = new Properties();

    private static String imdbUrl;
    private static String customerUrl;

    private static String postgreSQLDriver;
    private static String postgreSQLUser;
    private static String postgreSQLPassword;

    // DB Connection
    private Connection _imdb;
    private Connection _customer_db;

    // Canned queries

    private String _search_sql = "SELECT * FROM movie WHERE name ilike ? ORDER BY id";
    private PreparedStatement _search_statement;

    private String _director_mid_sql = "SELECT y.* "
                     + "FROM movie_directors x, directors y "
                     + "WHERE x.mid = ? and x.did = y.id";
    private PreparedStatement _director_mid_statement;
    
    // Query to help retrieve actors of a specific movie
    private String _actor_mid_sql = "SELECT y.* "
            + "FROM casts x, actor y "
            + "WHERE x.mid = ? and x.pid = y.id";
    private PreparedStatement _actor_mid_statement;
    
    // Check if a movie is available
    private String _rents_movie_sql = "SELECT cid " +
        "FROM movierentals WHERE mid = ? AND status = 'open'";
    private PreparedStatement _rents_movie_statement;
    
    /* Queries for fast search */
    /* Define any queries you use for fastsearch here */
    private String _director_fast_sql = "SELECT x.id, z.* "
                     + "FROM movie x, movie_directors y, directors z "
                     + "WHERE upper(x.name) like upper(?) and x.id = y.mid and y.did = z.id "
                     + "ORDER BY x.id";
    private PreparedStatement _director_fast_statement;

    private String _actor_fast_sql = "SELECT x.id, z.* "
                     + "FROM movie x, casts y, actor z "
                     + "WHERE upper(x.name) like upper(?) and x.id = y.mid and y.pid = z.id "
                     + "ORDER BY x.id";
    private PreparedStatement _actor_fast_statement;
    
    /* End of fast search queries*/
    

    private String _customer_login_sql = "SELECT * FROM customers WHERE login = ? and password = ?";
    private PreparedStatement _customer_login_statement;
    
    //Retrieve customer name information
    private String _customer_name_sql = "SELECT fname, lname " +
        "FROM customers WHERE cid = ?";
    private PreparedStatement _customer_name_statement;
    
    //Remaining rentals for a customer
    private String _still_rent_sql = "SELECT (" +
        "(SELECT p.max_movies FROM RentalPlans p WHERE p.pid = c.pid) - " +
        "(SELECT count(*) FROM MovieRentals r WHERE r.cid = c.cid AND r.status = 'open')) " +
        "FROM customers c WHERE c.cid = ?";
    private PreparedStatement _still_rent_statement;
    
    //query to retrieve all plans
    private String _plans_list_sql = "SELECT * FROM rentalplans";
    private PreparedStatement _plans_list_statement;
    
    //query to retrieve the rentals allowed in a plan
    private String _plans_maxrentals_sql = "SELECT max_movies FROM rentalplans WHERE pid = ?";
    private PreparedStatement _plans_maxrentals_statement;
    
    //query to retrieve the number of rentals of a specific customer
    private String _rentals_customer_sql = "SELECT count(*) FROM movierentals WHERE cid = ? AND status = 'open'";
    private PreparedStatement _rentals_customer_statement;
    
    //query to retrieve all rental mids of a specific customer
    private String _rentals_mid_list_sql = "SELECT mid FROM movierentals WHERE cid = ? AND status = 'open'";
    private PreparedStatement _rentals_mid_list_statement;
    
    //query to retrieve the name of a movie by id
    private String _movie_name_sql = "SELECT name FROM movie WHERE id = ?";
    private PreparedStatement _movie_name_statement;
    
    //check if the given plan ID is valid
    private String _valid_plan_sql = "SELECT pid " +
        "FROM rentalplans WHERE pid = ?";
    private PreparedStatement _valid_plan_statement;
    
    //check if the given movie id is valid
    private String _valid_movie_sql = "SELECT id " +
        "FROM movie WHERE id = ?";
    private PreparedStatement _valid_movie_statement;
    
    //update statement to switch plans
    private String _update_plan_sql = "UPDATE customers " +
        "SET pid = ? WHERE cid = ?";
    private PreparedStatement _update_plan_statement;
    
    //Format to display plan costs
    private NumberFormat feeFormat = new DecimalFormat("0.00"); 
    
    //rental query
    private String _rent_sql = "INSERT INTO movierentals " +
        "VALUES(?, ?, 'open')";
    private PreparedStatement _rent_statement;
    
    //return a movie
    private String _return_sql = "UPDATE movierentals " +
		"SET status = 'closed' WHERE cid = ? AND mid = ?";
	private PreparedStatement _return_statement;
    
    //Accounting for read-only transactions
    private String _begin_transaction_read_only_sql = "BEGIN TRANSACTION READ ONLY";
    private PreparedStatement _begin_transaction_read_only_statement;

    private String _begin_transaction_read_write_sql = "BEGIN TRANSACTION READ WRITE";
    private PreparedStatement _begin_transaction_read_write_statement;

    private String _commit_transaction_sql = "COMMIT TRANSACTION";
    private PreparedStatement _commit_transaction_statement;

    private String _rollback_transaction_sql = "ROLLBACK TRANSACTION";
    private PreparedStatement _rollback_transaction_statement;
     

    public Query() {
    }

    /**********************************************************/
    /* Connections to postgres databases */

    public void openConnection() throws Exception {
        configProps.load(new FileInputStream("dbconn.config"));
        
        
        imdbUrl        = configProps.getProperty("imdbUrl");
        customerUrl    = configProps.getProperty("customerUrl");
        postgreSQLDriver   = configProps.getProperty("postgreSQLDriver");
        postgreSQLUser     = configProps.getProperty("postgreSQLUser");
        postgreSQLPassword = configProps.getProperty("postgreSQLPassword");


        /* load jdbc drivers */
        Class.forName(postgreSQLDriver).newInstance();

        /* open connections to TWO databases: imdb and the customer database */
        _imdb = DriverManager.getConnection(imdbUrl, // database
                postgreSQLUser, // user
                postgreSQLPassword); // password

        _customer_db = DriverManager.getConnection(customerUrl, // database
                postgreSQLUser, // user
                postgreSQLPassword); // password
        _customer_db.setTransactionIsolation(4); // serializable isolation level
    }

    public void closeConnection() throws Exception {
        _imdb.close();
        _customer_db.close();
    }

    /**********************************************************/
    /* prepare all the SQL statements in this method.
      "preparing" a statement is almost like compiling it.  Note
       that the parameters (with ?) are still not filled in */

    public void prepareStatements() throws Exception {

        _search_statement = _imdb.prepareStatement(_search_sql);
        _director_mid_statement = _imdb.prepareStatement(_director_mid_sql);
        
        /* add any prepare statements for your fastsearch here */
        _director_fast_statement = _imdb.prepareStatement(_director_fast_sql);
        _actor_fast_statement = _imdb.prepareStatement(_actor_fast_sql);
        /* end of fastsearch prepare statements */
        
        _customer_login_statement = _customer_db.prepareStatement(_customer_login_sql);
        _begin_transaction_read_write_statement = _customer_db.prepareStatement(_begin_transaction_read_write_sql);
        _commit_transaction_statement = _customer_db.prepareStatement(_commit_transaction_sql);
        _rollback_transaction_statement = _customer_db.prepareStatement(_rollback_transaction_sql);
         

        /* add here more prepare statements for all the other queries you need */
        _actor_mid_statement = _imdb.prepareStatement(_actor_mid_sql);
        _rents_movie_statement = _customer_db.prepareStatement(_rents_movie_sql);
        
        _begin_transaction_read_only_statement = _customer_db.prepareStatement(_begin_transaction_read_only_sql);
        
        _customer_name_statement = _customer_db.prepareStatement(_customer_name_sql);
        _still_rent_statement = _customer_db.prepareStatement(_still_rent_sql);
        _plans_list_statement = _customer_db.prepareStatement(_plans_list_sql);
        _valid_plan_statement = _customer_db.prepareStatement(_valid_plan_sql);
        _update_plan_statement = _customer_db.prepareStatement(_update_plan_sql);
        _rent_statement = _customer_db.prepareStatement(_rent_sql);
        _valid_movie_statement = _imdb.prepareStatement(_valid_movie_sql);
        _return_statement = _customer_db.prepareStatement(_return_sql);
        _plans_maxrentals_statement = _customer_db.prepareStatement(_plans_maxrentals_sql);
        _rentals_customer_statement = _customer_db.prepareStatement(_rentals_customer_sql);
        _rentals_mid_list_statement = _customer_db.prepareStatement(_rentals_mid_list_sql);
        _movie_name_statement = _imdb.prepareStatement(_movie_name_sql);
    }


    /**********************************************************/
    /*  helper functions  */

    public int helper_compute_remaining_rentals(int cid) throws Exception {
        /* how many movies can she/he still rent ? */
        /* you have to compute and return the difference between the customer's plan
           and the count of oustanding rentals */
        _still_rent_statement.clearParameters();
        _still_rent_statement.setInt(1, cid);
        ResultSet still_set = _still_rent_statement.executeQuery();
        still_set.next();
        int c = still_set.getInt(1);
        still_set.close();
        return c;
    }

    public String helper_compute_customer_name(int cid) throws Exception {
        /* you find  the first + last name of the current customer */
        _customer_name_statement.clearParameters();
        _customer_name_statement.setInt(1, cid);
        ResultSet name_set = _customer_name_statement.executeQuery();
        String name;
        if (name_set.next())
        {
            name = name_set.getString("fname") + " " +
                name_set.getString("lname");
        } else {
            name = "customer not found";
        }
        name_set.close();
        return name;

    }

    public boolean helper_check_plan(int plan_id) throws Exception {
        /* is plan_id a valid plan id ?  you have to figure out */
        _valid_plan_statement.clearParameters();
        _valid_plan_statement.setInt(1, plan_id);
        ResultSet valid_set = _valid_plan_statement.executeQuery();
        boolean valid = valid_set.next();
        valid_set.close();
        return valid;
    }

    public boolean helper_check_movie(int mid) throws Exception {
        /* is mid a valid movie id ? you have to figure out  */
        _valid_movie_statement.clearParameters();
        _valid_movie_statement.setInt(1, mid);
        ResultSet valid_set = _valid_movie_statement.executeQuery();
        boolean valid = valid_set.next();
        valid_set.close();
        return valid;
    }

    private int helper_who_has_this_movie(int mid) throws Exception {
        /* find the customer id (cid) of whoever currently rents the movie mid; return -1 if none */
        _rents_movie_statement.clearParameters();
        _rents_movie_statement.setInt(1, mid);
        ResultSet rents_set = _rents_movie_statement.executeQuery();
        int cid;
        if (rents_set.next())
        {
            cid = rents_set.getInt("cid");
        } else {
            cid = -1;
        }
        rents_set.close();
        return cid;
    }

    /**********************************************************/
    /* login transaction: invoked only once, when the app is started  */
    public int transaction_login(String name, String password) throws Exception {
        /* authenticates the user, and returns the user id, or -1 if authentication fails */

        
        int cid;
        _begin_transaction_read_only_statement.executeUpdate();
        _customer_login_statement.clearParameters();
        _customer_login_statement.setString(1,name);
        _customer_login_statement.setString(2,password);
                
        ResultSet cid_set = _customer_login_statement.executeQuery();
        
        if (cid_set.next()) cid = cid_set.getInt(1);
        else cid = -1;
        _commit_transaction_statement.executeUpdate();
        return(cid);         
    }

    public void transaction_personal_data(int cid) throws Exception {
        /* print the customer's personal data: name, and plan number */
        int remainingRentals = helper_compute_remaining_rentals(cid);    
        String customer_name = helper_compute_customer_name(cid);
        System.out.println("Customer "+customer_name);
        System.out.println("You have "+remainingRentals+" available movies for rent");
    }


    /**********************************************************/
    /* main functions in this application: */

    public void transaction_search(int cid, String movie_title)
            throws Exception {
        /* searches for movies with matching titles: SELECT * FROM movie WHERE name LIKE movie_title */
        /* prints the movies, directors, actors, and the availability status:
           AVAILABLE, or UNAVAILABLE, or YOU CURRENTLY RENT IT */

        /* Start the timer*/
        long startTime = System.currentTimeMillis();

        /* set the first (and single) '?' parameter */
        _search_statement.clearParameters();
        _search_statement.setString(1, '%' + movie_title + '%');

        ResultSet movie_set = _search_statement.executeQuery();
        while (movie_set.next()) {
            int mid = movie_set.getInt(1);
            System.out.println("ID: " + mid + " NAME: "
                    + movie_set.getString(2) + " YEAR: "
                    + movie_set.getString(3));
            /* do a dependent join with directors */
            _director_mid_statement.clearParameters();
            _director_mid_statement.setInt(1, mid);
            ResultSet director_set = _director_mid_statement.executeQuery();
            while (director_set.next()) {
                System.out.println("\t\tDirector: " + director_set.getString(3)
                        + " " + director_set.getString(2));
            }
            director_set.close();
            
            /* now you need to retrieve the actors, in the same manner */
            _actor_mid_statement.clearParameters();
            _actor_mid_statement.setInt(1, mid);
            ResultSet actor_set = _actor_mid_statement.executeQuery();
            while (actor_set.next()) {
                System.out.println("\t\tActor: "
                        + actor_set.getString("fname") + " "
                        + actor_set.getString("lname"));
            }
            actor_set.close();
            
            /* then you have to find the status: of "AVAILABLE" "YOU HAVE IT", "UNAVAILABLE" */
            int hasMovie = helper_who_has_this_movie(mid);
            if (hasMovie == -1)
                System.out.println("\t\tAVAILABLE");
            else if (hasMovie == cid)
                System.out.println("\t\tYOU HAVE IT");
            else
                System.out.println("\t\tUNAVAILABLE");
        }
        System.out.println();
        
        /* End the timer*/
        long endTime = System.currentTimeMillis();
        
        System.out.println("Search completed in " + ((endTime-startTime)/1000.00) + " seconds");
        
        System.out.println();
    }

    
    public void transaction_choose_plan(int cid, int pid) throws Exception {
                
        /* check how many movies the customer is renting */
        _rentals_customer_statement.clearParameters();
        _rentals_customer_statement.setInt(1, cid);
        ResultSet rental_set = _rentals_customer_statement.executeQuery();
        
        rental_set.next();
        int c1 = rental_set.getInt(1);
        rental_set.close();
        
        /* check how many rentals the requested plan allows */
        _plans_maxrentals_statement.clearParameters();
        _plans_maxrentals_statement.setInt(1, pid);
        ResultSet newmax_set = _plans_maxrentals_statement.executeQuery();
        
        newmax_set.next();
        int c2 = newmax_set.getInt(1);
        newmax_set.close();
        
        
        int remaining = c2 - c1;
        if (remaining < 0) {
            System.out.println("You cannot switch to this plan unless you return some movies.");
        } else {
            /* updates the customer's plan to pid */
            _update_plan_statement.clearParameters();
            _update_plan_statement.setInt(1, pid);
            _update_plan_statement.setInt(2, cid);
            _update_plan_statement.executeUpdate();
        }
    }
    
    

    public void transaction_list_plans() throws Exception {
        /* print all available plans: SELECT * FROM plan */
        ResultSet plans_set = _plans_list_statement.executeQuery();
        while (plans_set.next())
        {
            System.out.println(plans_set.getInt("pid") + "\t" + String.format("%-17s",plans_set.getString("name")) + "\t" + 
                    "max " + plans_set.getInt("max_movies") + " movies\t" + 
                    "$" + feeFormat.format(plans_set.getDouble("fee")));
        }
        plans_set.close();
    }
    
    public void transaction_list_user_rentals(int cid) throws Exception {
        /* print all movies rented by the current user*/
        System.out.println("You are currently renting the following movies:");
        _rentals_mid_list_statement.clearParameters();
        _rentals_mid_list_statement.setInt(1, cid);
        ResultSet rented_set = _rentals_mid_list_statement.executeQuery();
        while (rented_set.next())
        {
            int mid = rented_set.getInt("mid");
            
            _movie_name_statement.clearParameters();
            _movie_name_statement.setInt(1, mid);
            ResultSet movieName = _movie_name_statement.executeQuery();
            movieName.next();
            System.out.println(mid + "\t" + movieName.getString("name"));
        }
    }

    public void transaction_rent(int cid, int mid) throws Exception {
        /* rent the movie mid to the customer cid */
        
        _begin_transaction_read_write_statement.executeUpdate();
        
        if(!helper_check_movie(mid)){
            _rollback_transaction_statement.executeUpdate();
            System.out.println("The movie you requested does not exist.");
            return;
        }
        
        int remaining = helper_compute_remaining_rentals(cid);
        if (remaining <= 0)
        {
            _rollback_transaction_statement.executeUpdate();
            System.out.println("You cannot rent more movies with your current plan.");
            return;
        }
        
        int hasMovie = helper_who_has_this_movie(mid);
        if (hasMovie == -1)
        {
            _rent_statement.clearParameters();
            _rent_statement.setInt(1, mid);
            _rent_statement.setInt(2, cid);
            _rent_statement.executeUpdate();
            
            _commit_transaction_statement.executeUpdate();
            return;
        }
        _rollback_transaction_statement.executeUpdate();
        
        if (hasMovie == cid)
        {
            System.out.println("You already rent this movie.");
        } else {
            System.out.println("Somebody else is already renting this movie.");
        }
    }

    public void transaction_return(int cid, int mid) throws Exception {
        /* return the movie mid by the customer cid */
        
        _begin_transaction_read_write_statement.executeUpdate();
        
        int hasMovie = helper_who_has_this_movie(mid);
        if (hasMovie == cid)
        {
            _return_statement.clearParameters();
            _return_statement.setInt(1, cid);
            _return_statement.setInt(2, mid);
            _return_statement.executeUpdate();
            
            _commit_transaction_statement.executeUpdate();
            return;
        }
        _rollback_transaction_statement.executeUpdate();
        
        System.out.println("You are not currently renting this movie.");
    }

    public void transaction_fast_search(int cid, String movie_title)
            throws Exception {
        /* like transaction_search, but pushes some of the join logic to the database */
        
        /* Start the timer*/
            long startTime = System.currentTimeMillis();
        
        /* Insert your code for fastsearch here */
        
        _search_statement.clearParameters();
        _search_statement.setString(1, '%' + movie_title + '%');
        ResultSet movie_set = _search_statement.executeQuery();

        _director_fast_statement.clearParameters();
        _director_fast_statement.setString(1, '%' + movie_title + '%');
        ResultSet director_set = _director_fast_statement.executeQuery();

        _actor_fast_statement.clearParameters();
        _actor_fast_statement.setString(1, '%' + movie_title + '%');
        ResultSet actor_set = _actor_fast_statement.executeQuery();

        while (movie_set.next()){
            int mid = movie_set.getInt(1);
            System.out.println("ID: " + mid + " NAME: "
                    + movie_set.getString(2) + " YEAR: "
                    + movie_set.getString(3));
        
            while (director_set.next()){
                if (director_set.getInt(1) == mid) {
                    System.out.println("\t\tDirector: " + director_set.getString(4)
                            + " " + director_set.getString(3));
                }
                else {
                    break;
                }
            } 

            while (actor_set.next()){
                if (actor_set.getInt(1) == mid) {
                    System.out.println("\t\tActor: " + actor_set.getString(4)
                            + " " + actor_set.getString(3));
                }
                else {
                    break;
                } 
            } 
        }
        movie_set.close();
        director_set.close();
        actor_set.close();
        System.out.println();

        
        /* End of fastsearch code */
        
         /* End the timer*/
            long endTime = System.currentTimeMillis();

            System.out.println("Search completed in " + ((endTime-startTime)/1000.00) + " seconds");

            System.out.println();
    }

}
