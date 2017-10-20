import org.rosuda.JRI.Rengine;

public class Temp {

    public static void main(String a[]) {

    	//System.setProperty("java.library.path", "/Library/Frameworks/R.framework/Resources/library/rJava/jri/");
    	System.out.println(System.getProperty("java.library.path"));
        // Create an R vector in the form of a string.
        String javaVector = "c(1,2,3,4,5)";

        // Start Rengine.
        Rengine engine = new Rengine(new String[] { "--no-save" }, false, null);

        // The vector that was created in JAVA context is stored in 'rVector' which is a variable in R context.
        engine.eval("rVector=" + javaVector);
        
        //Calculate MEAN of vector using R syntax.
        engine.eval("meanVal=mean(rVector)");
        
        //Retrieve MEAN value
        double mean = engine.eval("meanVal").asDouble();
        
        //Print output values
        System.out.println("Mean of given vector is=" + mean);

    }
}