package com.mapr.data.sputnik.util;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.*;
import java.io.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RandomDataGen {
	private static final Logger log = LogManager.getLogger(RandomDataGen.class);
	
	private static RandomDataGen instance;
	
	private Random random = new Random();
	private final MathContext mathContext = new MathContext(28, RoundingMode.HALF_UP);
	private InputStream dictFile;
	private static List<String> dictWordList = null;

	private String ROW_SEPARATOR = "\n";
	private String COLUMN_SEPARATOR = ",";//"\u0001";
	private String QUOTE_STRING = "\"";

	private final String ALPHA_STRING = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	private final String NUMERIC_STRING = "1234567890";
	private final String SPECIAL_STRING = "`-=~!@#$%^&*()_+[]{};':,|./<>?";
	private char[] SEED_STRING = (ALPHA_STRING + NUMERIC_STRING + SPECIAL_STRING).toCharArray();
	private char[] ALPHANUM_SEED_STRING = (ALPHA_STRING + NUMERIC_STRING).toCharArray();
	private char[] ALPHA_SEED_STRING = (ALPHA_STRING.substring(25)).toCharArray();
	private boolean quoted = false;
	
	private RandomDataGen(long seed) {
		if(seed != 0)
			random.setSeed(seed);
		initDict();
		log.info("Random Data Generation initiated with seed[" + getSeed() + "]");
	}

	public static RandomDataGen getInstance() {
		return getInstance(0);
	}
	
	public synchronized static RandomDataGen getInstance(long seed) {
		if (instance == null) {
			instance = new RandomDataGen(seed);
		}
		return instance;
	}
	
	public Random getRandom(){
		return random;
	}

	public String createRandomString(int len){			
		return createRandomString(len, false);		
	}

	public String randAlphanum(int len, boolean space){
		StringBuffer out=new StringBuffer();
		while(out.length() < len){
			int idx=Math.abs(( random.nextInt() % ALPHANUM_SEED_STRING.length ));
			out.append(ALPHANUM_SEED_STRING[idx]);
			if(space && out.length() < len){
				int lRand = randInt(0, len);
				if(lRand > len/5 && lRand < len/4)
					out.append(" ");
			}
		}			
		return out.toString();
	}
	
	public String randAlpha(int len, boolean space, boolean uppercase){
		StringBuffer out=new StringBuffer();
		while(out.length() < len){
			int idx=Math.abs(( random.nextInt() % ALPHA_SEED_STRING.length ));
			out.append(ALPHA_SEED_STRING[idx]);
			if(space && out.length() < len){
				int lRand = randInt(0, len);
				if(lRand > len/5 && lRand < len/4)
					out.append(" ");
			}
		}			
		return uppercase?out.toString().toUpperCase():out.toString();
	}
	
	public String createRandomString(int len, boolean space){
		StringBuffer out=new StringBuffer();
		while(out.length() < len){
			int idx=Math.abs(( random.nextInt() % SEED_STRING.length ));
			if(COLUMN_SEPARATOR.equals(SEED_STRING[idx]) || ROW_SEPARATOR.equals(SEED_STRING[idx]))
				continue;
			if(quoted && QUOTE_STRING.equals(SEED_STRING[idx]))
				continue;
			out.append(SEED_STRING[idx]);
			if(space && out.length() < len){
				int lRand = randInt(0, len);
				if(lRand > len/5 && lRand < len/4)
					out.append(" ");
			}
		}			
		return out.toString();
	}

	public String createRandomUString(int len, boolean space, int lang){
		StringBuffer out=new StringBuffer();
		while((out.length()*2) < len){
			String str = getUnicodeChar(lang);
			out.append(str);
			if(space && (out.length()*2) < len){
				int lRand = randInt(0, len);
				if(lRand > len/5 && lRand < len/4)
					out.append(" ");
			}
		}			
		return out.toString();
	}

	private String getUnicodeChar(int lang){
		int i = 0;
		if(lang == 0){ //Devanagiri
			i = randInt(0x0900, 0x097F);				
		}else if(lang == 1){ // Russian?
			i = randInt(0x0400, 0x045F);				
		}else if(lang == 2){ // Kannada
			i = randInt(0x0C85, 0x0D00);				
		} else if(lang == 3){ // Han
			i = randInt(0x4E00, 0x9FFF);
		}
		String str = Character.toString((char)i);			
		return str;
	}

	public String createIntHexRandString(int length) {		
		StringBuilder sb = new StringBuilder();
		while (sb.length() < length) {
			sb.append(Integer.toHexString(random.nextInt()));
		}
		return sb.toString();
	}

	public String createLongtHexRandString() {
		String str = Long.toHexString(Double.doubleToLongBits(Math.random()));
		return str;
	}

	public String getDictionaryString(int len){
		StringBuffer out=new StringBuffer();
		while(out.length() < len){
			String str = dictWordList.get(randInt(0, dictWordList.size()-1)); 
			
			if(out.length() + str.length() < len)
				out.append(str);
			else
				out.append(str.substring(0, len - out.length()));
			if(out.length() < len)
				out.append(" ");			
		}			
		return out.toString();
	}
	
	public int randInt(int lo, int hi)
	{
		int n = hi - lo + 1;
		int i = random.nextInt() % n;
		if (i < 0)
			i = -i;
		return lo + i;
	}

	public long randLong(long lo, long hi){
		long n = hi - lo + 1;
		long bi = random.nextLong() % n;
		if (bi < 0)
			bi = -bi;
		return lo + bi;	
	}

	public boolean randBoolean(){
		int i = random.nextInt();
		return (i>0&&i/2==0?true:false);
	}
	
	public int randInt(){
		return random.nextInt();        
	}

	public String randDouble(double min, double max){
		double range = max - min;
        double scaled = random.nextDouble() * range;
        double shifted = scaled + min;
        return String.format("%.4f", shifted);
        
	}
	public String randDouble(){
		int power = randInt(0, 308);
		long factor = (long) Math.pow(10, 15);
		double d = random.nextDouble();
		long tmp = (long)(d * factor);
		d = ((double) tmp / factor) * 10;
		if(power % 8 > 4) { d=-d; tmp=-tmp; }
		String dbl = "";
		if(power < 15){
			dbl += tmp;
		}else{
			DecimalFormat df = new DecimalFormat("#.##############");
			dbl += df.format(d)+(power>0?"E"+power:"");
		}
		return dbl;			        
	}

	public BigDecimal randDecimal(){
		int power = randInt(0, 28);
		double d = random.nextDouble() * Math.pow(10, power);
		if(power % 8 > 4) d=-d;
		BigDecimal b = new BigDecimal(d, mathContext);			
		return b;
	}

	public byte randByte(){
		byte[] bytes = new byte[1];
		random.nextBytes(bytes);
		return bytes[0];
	}

	public short randShort(){
		return (short)random.nextInt(1<<16);
	}

	public int randDate(){
		return (int) (randDateTime().getTime() / 86400000);
	}

	public long randTimestamp(){
		return randDateTime().getTime();
	}

	@SuppressWarnings("deprecation")
	public Date randDateTime(){
		int y = randInt(0, 9999)-1900;
		int m = randInt(0, 11);		
		GregorianCalendar gc = new GregorianCalendar(y, m, 1);
		int d = randInt(1, gc.getActualMaximum(Calendar.DAY_OF_MONTH));
		if(d==29 && m==1 && y < 0) d=28;
		if(d==31 && m==11 && (y+1900)%400==0) d=30; 
		int h = randInt(0, 23);
		int mi = randInt(0, 59);
		int s = randInt(0, 59);
		int n = randInt(00000, 999999999);
		Timestamp t = new Timestamp(y,m,d,h,mi,s,n);		
		return new Date(t.getTime());
	}

	public long getSeed() {
		byte[] ba0, ba1, bar;
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream(128);
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(new Random(0));
			ba0 = baos.toByteArray();
			baos = new ByteArrayOutputStream(128);
			oos = new ObjectOutputStream(baos);
			oos.writeObject(new Random(-1));
			ba1 = baos.toByteArray();
			baos = new ByteArrayOutputStream(128);
			oos = new ObjectOutputStream(baos);
			oos.writeObject(random);
			bar = baos.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException("IOException: " + e);
		}
		if (ba0.length != ba1.length || ba0.length != bar.length)
			throw new RuntimeException("bad serialized length");
		int i = 0;
		while (i < ba0.length && ba0[i] == ba1[i]) {
			i++;
		}
		int j = ba0.length;
		while (j > 0 && ba0[j - 1] == ba1[j - 1]) {
			j--;
		}
		if (j - i != 6)
			throw new RuntimeException("6 differing bytes not found");
		// The constant 0x5DEECE66DL is from
		// http://download.oracle.com/javase/6/docs/api/java/util/Random.html .
		return ((bar[i] & 255L) << 40 | (bar[i + 1] & 255L) << 32 |
				(bar[i + 2] & 255L) << 24 | (bar[i + 3] & 255L) << 16 |
				(bar[i + 4] & 255L) << 8 | (bar[i + 5] & 255L)) ^ 0x5DEECE66DL;
	}
	
	private void initDict(){
		if(dictFile == null){
			dictFile = this.getClass().getClassLoader().getResourceAsStream("words");
			assert dictFile!=null : "Dictionary file 'words' doesn't exist!";
		}
		dictWordList = new ArrayList<String>(); 
		BufferedReader br = new BufferedReader(new InputStreamReader(dictFile));
        String line;
        try {
			while((line = br.readLine()) != null) {
				dictWordList.add(line);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

