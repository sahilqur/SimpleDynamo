package edu.buffalo.cse.cse486586.simpledynamo;


import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

public class SimpleDynamoActivity extends Activity {
	private Button Put1;
	private Button Put2;
	private Button Put3;	
	private Button LDump;
	private Button Get;		
	String msg2,key,value;
	public SQLiteDatabase db;
	Cursor resultCursor=null,c=null;	
	public static final Uri CONTENT_URI = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
	
	public class put implements Runnable {
		String b;
		put(String bt) {
			b=bt;
		}
		public void run() {
			try {						    	
    			for(int i=0;i<20;i++) {
    				key=Integer.toString(i);
    				value=b+key;
    				ContentValues cv=new ContentValues();
    				cv.put(MyDbHelper.KEY,key);
    				cv.put(MyDbHelper.VALUE,value);    				
    				Uri newUri = getContentResolver().insert(CONTENT_URI,cv);
    				try {
    					Thread.sleep(1000);
    					} catch (InterruptedException e) {
    						e.printStackTrace();
    					}
    			}
    		}catch (Exception e) {
    			e.printStackTrace();
    		}
			
		}
	}
	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
    
		final TextView tv = (TextView) findViewById(R.id.textView1);
		Put1=(Button) findViewById(R.id.button1);
		Put2=(Button) findViewById(R.id.button2);
		Put3=(Button) findViewById(R.id.button3);
		LDump=(Button) findViewById(R.id.button4);
		Get=(Button) findViewById(R.id.button5);		
		tv.setMovementMethod(new ScrollingMovementMethod());
		
		Put1.setOnClickListener(new Button.OnClickListener() {
        	public void onClick(View v) {
        		Thread p1 = new Thread(new put("Put1"));
                p1.start();
        	}
		});
				
		
		Put2.setOnClickListener(new Button.OnClickListener() {
        	public void onClick(View v) {
        		Thread p1 = new Thread(new put("Put2"));
                p1.start();
        	}
		});
		
		Put3.setOnClickListener(new Button.OnClickListener() {
        	public void onClick(View v) {
        		Thread p1 = new Thread(new put("Put3"));
                p1.start();
        	}
		});
		
		Get.setOnClickListener(new Button.OnClickListener() {
        	public void onClick(View v) {
        		tv.setText("");
        		try {        		
        			for(int i=0;i<20;i++) {
        				String key= Integer.toString(i);
        				resultCursor = getContentResolver().query(SimpleDynamoProvider.CONTENT_URI, null,key, null, null);
        				resultCursor.moveToFirst();        					
        				int keyIndex = resultCursor.getColumnIndex(MyDbHelper.KEY);
        				int valueIndex = resultCursor.getColumnIndex(MyDbHelper.VALUE);
        				String returnKey = resultCursor.getString(keyIndex);
        				String returnValue = resultCursor.getString(valueIndex);        				
        				tv.append("\n"+returnKey+" "+returnValue);  	        			      		     			     			        
        				resultCursor.close();
        				try {
        				    Thread.sleep(1000);
        				} catch(InterruptedException ex) {
        				    Thread.currentThread().interrupt();
        				}
        			}        			
        		}catch(Exception e) {
        			e.printStackTrace();
        		}
        	}
		});
		
		LDump.setOnClickListener(new Button.OnClickListener() {
        	public void onClick(View v) {
        		resultCursor = getContentResolver().query(SimpleDynamoProvider.CONTENT_URI,null,null,null,null);
        		tv.setText("");
        		int index_value = resultCursor.getColumnIndex(MyDbHelper.VALUE);
        		int index_key = resultCursor.getColumnIndex(MyDbHelper.KEY);        		
        		if(resultCursor==null) {
        			tv.append("\n No values in AVD");
        		}
        		else if(resultCursor!=null) {
        			while(resultCursor.moveToNext()) {        				
        				tv.append("\n"+resultCursor.getString(index_key)+" "+resultCursor.getString(index_value));
        			}
        		}
        		resultCursor.close();
        	}
		});

	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
}
