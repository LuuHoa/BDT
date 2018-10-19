package Comsumer.Bike;

public class test {

	
 	Integer User_ID;
  	String Product_ID;
  	String Gender;
  	String Age;
  	Integer Occupation;
  	String City_Category;
  	String Stay_In_Current_City_Years;
  	Integer Marital_Status;
  	Integer Product_Category_1;
  	Integer Product_Category_2;
  	Integer Product_Category_3;
  	Double Purchase;
  	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String data = "1000001,P00069042,F,0-17,10,A,2,0,3,,,8370";
		String[] elements = data.split(",");
		for( String e : elements)
		{
		System.out.println(e);
		}
		
		test row = new test();
		row.User_ID = myparseInt(elements[0]);
  		row.Product_ID	= elements[1];
  		row.Gender	= elements[2];
  		row.Age	= elements[3];
  		row.Occupation	= myparseInt(elements[4]);
  		row.City_Category	= elements[5];
  		row.Stay_In_Current_City_Years	= elements[6];
  		row.Marital_Status	= myparseInt(elements[7]);
  		row.Product_Category_1	= myparseInt(elements[8]);
  		row.Product_Category_2	= myparseInt(elements[9]);
  		row.Product_Category_3	= myparseInt(elements[10]);
  		row.Purchase = Double.parseDouble(elements[11]);
		
	}
	static Integer myparseInt(String value) {
    	Integer parsedValue = 0;
    	try {
    		parsedValue = Integer.parseInt(value);
    	} catch (Exception e) {
    	}
    	return parsedValue;
    }
    

}
