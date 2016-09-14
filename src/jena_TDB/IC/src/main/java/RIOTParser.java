import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.jena.atlas.io.PeekReader;
import org.apache.jena.atlas.io.Writer2;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.lang.LabelToNode;
import org.apache.jena.riot.lang.LangNTuple;
import org.apache.jena.riot.out.CharSpace;
import org.apache.jena.riot.out.NodeFormatterNT;
import org.apache.jena.riot.system.IRIResolver;
import org.apache.jena.riot.system.ParserProfile;
import org.apache.jena.riot.system.ParserProfileBase;
import org.apache.jena.riot.system.Prologue;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.tokens.Token;
import org.apache.jena.riot.tokens.TokenType;
import org.apache.jena.riot.tokens.Tokenizer;
import org.apache.jena.riot.tokens.TokenizerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;


public class RIOTParser {
	static NodeFormatterNT nodeFmt = new NodeFormatterNT(CharSpace.UTF8) ;
	public static void main(String[] args) throws IOException {
		System.out.println("RIOT PARSER ");
		
//		args = new String[]{"data.nq", "d.nt"};
		
		
		
		InputStream is;
		if(args.length==1){
			is = System.in;
		}
		else{
			String inFile = args[0];
			
			if(inFile.endsWith(".gz")){
				is = new GZIPInputStream(new FileInputStream(new File(inFile)));
			}else{
				is =new FileInputStream(new File(inFile));
			}
		}
		PeekReader peekReader = PeekReader.makeUTF8(is) ;
		Tokenizer tokenizer = new MyTokenizer(peekReader) ;
		ParserProfileBase profile = new ParserProfileBase(new Prologue(null, IRIResolver.createNoResolve()), null, LabelToNode.createUseLabelEncoded()) ;
		
		MyNQuadParser parser = new MyNQuadParser(tokenizer, profile, null) ;
		parser.setSkipOnBadTerm(true);
		
		
		File outFile = new File(args[0]);
		if(args.length==2){
			outFile = new File(args[1]);
		}
		System.out.println("Writting to "+outFile.getAbsolutePath());
		OutputStream os = new FileOutputStream(outFile);
		if(outFile.toString().endsWith(".gz")){
			os = new GZIPOutputStream(os);
		}
		PrintWriter pw = new PrintWriter(os);
		Writer2 out = Writer2.wrap(pw);
		long start = System.currentTimeMillis();
		int total=0, written=0, errors=0;
		while(parser.hasNext()){
			total++;
			try{
				Quad q = parser.next();
				
				print(out, q );
				
				if(total%100000==0){
					System.out.println("Parsed "+total+" statements");
				}
				written++;
			}catch(Exception e){
//				System.out.println("Error in line "+total);
				errors++;
				System.out.println(e.getMessage());
			}
		}
		out.close();
		os.close();
		long end = System.currentTimeMillis();
		
		System.out.println("Parsing done!");
		System.out.println("Time elapsed: "+(end-start)+" ms");
		System.out.println("Total statements "+total);
		System.out.println("Written: "+written);
		System.out.println("Total errors "+ (errors));
	}

	private static void print(Writer2 out, Quad q) {
		nodeFmt.format(out, q.getSubject());
		out.print(" ") ;
		nodeFmt.format(out, q.getPredicate()) ;
		out.print(" ") ;
		nodeFmt.format(out, q.getObject()) ;
		out.print(" .\n") ;
		out.flush();
	}


}
class MyNQuadParser extends LangNTuple<Quad>{
	// Null for no graph.
	private Node currentGraph = null ;

	public MyNQuadParser(Tokenizer tokens, ParserProfile profile, StreamRDF dest) {
		super(tokens, profile, dest);
	}

	@Override
	public Lang getLang()   { return RDFLanguages.NQUADS ; }


	@Override
	protected Quad parseOne() {
		Token xToken=null;
		try{
			Token sToken = nextToken() ;
			if ( sToken.getType() == TokenType.EOF )
				exception(sToken, "Premature end of file: %s", sToken) ;

			Token pToken = nextToken() ;
			if ( pToken.getType() == TokenType.EOF )
				exception(pToken, "Premature end of file: %s", pToken) ;

			Token oToken = nextToken() ;
			if ( oToken.getType() == TokenType.EOF )
				exception(oToken, "Premature end of file: %s", oToken) ;

			xToken = nextToken() ;    // Maybe DOT
			if ( xToken.getType() == TokenType.EOF )
				exception(xToken, "Premature end of file: Quad not terminated by DOT: %s", xToken) ;

			// Process graph node first, before S,P,O
			// to set bnode label scope (if not global)
			Node c = null ;

			if ( xToken.getType() != TokenType.DOT )
			{
				// Allow bNodes for graph names.
				checkIRIOrBNode(xToken) ;
				// Allow only IRIs
				//checkIRI(xToken) ;
				c = tokenAsNode(xToken) ;
				xToken = nextToken() ;
				currentGraph = c ;
			}
			else
			{
				c = Quad.defaultGraphNodeGenerated ;
				currentGraph = null ;
			}

			// createQuad may also check but these checks are cheap and do form syntax errors.
			checkIRIOrBNode(sToken) ;
			checkIRI(pToken) ;
			checkRDFTerm(oToken) ;
			// xToken already checked.

			Node s = tokenAsNode(sToken) ;
			Node p = tokenAsNode(pToken) ;
			Node o = tokenAsNode(oToken) ;

			// Check end of tuple.

			if ( xToken.getType() != TokenType.DOT )
				exception(xToken, "Quad not terminated by DOT: %s", xToken) ;

			return profile.createQuad(c, s, p, o, sToken.getLine(), sToken.getColumn()) ;
		}catch(Exception e){
			MyTokenizer mt = (MyTokenizer) tokens;
//			System.out.println("SKIP");
			mt.skipLine();
		
			Token t = t=nextToken();
			while(t!=null &&  (t.getType() != TokenType.DOT )){
//				System.out.println("t" +t);
				t = nextToken();
			}
//			System.out.println(t);
			
//			nextToken();
//			MyTokenizer t = (MyTokenizer) tokens;
//			System.out.println("SKIP");
//			t.skipLine();
//			nextToken();
//			while(xToken != null &&  (xToken.getType() != TokenType.DOT )){
//				xToken = nextToken();
//			}
//			if(xToken == null){
//				MyTokenizer t = (MyTokenizer) tokens;
//				System.out.println("SKIP");
//				t.skipLine();
//			}
////				Token t = nextToken();
////				System.out.println(t);
////				tokens.peek();
////				Token t1 = nextToken();
////				System.out.println(t1);
////				
////			}
			throw e;
		}
	}

	@Override
    protected final Node tokenAsNode(Token token) 
    {
        return profile.create(currentGraph, token) ;
    }

	/** Method to parse the whole stream of triples, sending each to the sink */ 
	@Override
	protected final void runParser()
	{
		while(hasNext())
		{
			Quad x = parseOne() ;
			if ( x != null )
				dest.quad(x) ;
		}
	}


}
