

public class RIOParser {
}
//	public static void main(String[] args) throws IOException {
//		args = new String[]{"data1.nt", "data_c.nt"};
//		String in = args[0];
//		String out = args[1];
//
//		InputStream is;
//		if(in.endsWith(".gz")){
//			is = new GZIPInputStream(new FileInputStream(new File(in)));
//		}else{
//			is =new FileInputStream(new File(in));
//		}
//		RDFParser p=null;
//		if(in.contains(".nt")){
//			TurtleParserFactory pf = new TurtleParserFactory();
////			NTriplesParserFactory pf = new NTriplesParserFactory();
//			p = pf.getParser();
//		}else if(in.contains(".nq")){
//			NQuadsParserFactory quadf = new NQuadsParserFactory();
//			p = quadf.getParser();
//		}
//		
//		
//
//
//		NTriplesWriterFactory nfw = new NTriplesWriterFactory();
//		final RDFWriter w = nfw.getWriter(new FileOutputStream(new File(out)));
//		
//		
//		ErrorListener el = new ErrorListener();
//		p.setParseErrorListener(el);
//		p.setVerifyData(true);
//		p.setStopAtFirstError(false);
//
//		NTriplesWriter ww = new NTriplesWriter(w);
//		p.setRDFHandler(ww);
//		
//
//		long start = System.currentTimeMillis();
//
//		try {
//			p.parse(is, "http://example.org");
//		}
//		catch (Exception e) {
//			e.printStackTrace();
//		}
//		long end = System.currentTimeMillis();
//		int [] counts = el.getCount();
//		int t = counts[0]+counts[1]+counts[2];
//		
//		System.out.println("Parsing done!");
//		System.out.println("Time elapsed: "+(end-start)+" ms");
//		System.out.println("Total statements "+(t+ww.getCount()));
//		System.out.println("Written: "+ww.getCount());
//		System.out.println("Total errors "+ (t));
//		System.out.println("___ ERROR COUNTS ____ ");
//		System.out.println("WARNINGS: "+counts[0]);
//		System.out.println("ERRORS: "+counts[1]);
//		System.out.println("FATAL: "+counts[2]);
//
//
//	}
//
//}
//class NTriplesWriter implements RDFHandler{
//
//	private RDFWriter _w;
//	private int _c;
//
//	public NTriplesWriter(RDFWriter w) {
//		_w =w;
//		_c=0;
//	}
//
//	@Override
//	public void startRDF() throws RDFHandlerException {
//		// TODO Auto-generated method stub
//		_w.startRDF();
//	}
//
//	@Override
//	public void endRDF() throws RDFHandlerException {
//		// TODO Auto-generated method stub
//		_w.endRDF();
//	}
//
//	@Override
//	public void handleNamespace(String prefix, String uri)
//			throws RDFHandlerException {
//		// TODO Auto-generated method stub
//		_w.handleNamespace(prefix, uri);
//	}
//
//	@Override
//	public void handleStatement(Statement st) throws RDFHandlerException {
//		// TODO Auto-generated method stub
//		_w.handleStatement(st);
//		_c++;
//		if(_c % 100000 ==0){
//			System.out.println("Parsed "+_c+" statements");
//		}
//	}
//
//	@Override
//	public void handleComment(String comment) throws RDFHandlerException {
//		// TODO Auto-generated method stub
//		_w.handleComment(comment);
//	}
//
//	public int getCount(){ return _c;}
//
//}
//
//
//class ErrorListener implements ParseErrorListener{
//
//	int [] counts = {0,0,0};
//
//	@Override
//	public void warning(String msg, int lineNo, int colNo) {
//		// TODO Auto-generated method stub
//		counts[0]++;
//	}
//
//	@Override
//	public void error(String msg, int lineNo, int colNo) {
//		// TODO Auto-generated method stub
//		counts[1]++;
//	}
//
//	@Override
//	public void fatalError(String msg, int lineNo, int colNo) {
//		// TODO Auto-generated method stub
//		counts[2]++;
//	}
//
//	public int[] getCount(){ return counts;}
//
//}
