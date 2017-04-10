package org.hive.udfs.myudfs;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
 
/*
 * Class that implements our udf.
 * Will be loaded once per mapper and/or reducer
 */
public final class WordCount extends UDF {

  /*
   * Method that is executed on each line of the input
   */
  public Text evaluate(final Text text) throws Exception {
    if (text == null) { return null; }
    
    if (true)
    	throw new Exception("Your first (but not last) MapredTask error !");
    
    String s = text.toString();
    
    if (s.isEmpty())
    	return new Text("0");
    
    s = s.replace("\\n", " ");
    String[] split = s.split("\\W");
    
    return new Text(String.valueOf(split.length));
  }
  
  //For testing purpose.
  public static void main(String[] args) throws Exception {
	  
	  WordCount udf = new WordCount();
	  Text t = new Text("this is a test!");
	  System.out.println(udf.evaluate(t));
  }
}