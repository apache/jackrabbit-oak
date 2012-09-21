package org.apache.jackrabbit.mk.testing;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.util.Chronometer;
import org.apache.jackrabbit.mk.util.Configuration;
import org.apache.jackrabbit.mk.util.MkConfigProvider;
import org.junit.Before;
import org.junit.BeforeClass;



public class MicroKernelTestBase {

	static Initializator initializator;
	public MicroKernel mk;
	public static Configuration conf;
	public Chronometer chronometer;

	/**
	 * Creates a microkernel collection with only one microkernel.
	 * 
	 * @throws Exception
	 */
	@BeforeClass
	public static void beforeSuite() throws Exception {
		
		String mktype = System.getProperty("mk.type");
		initializator = (mktype == null || mktype.equals("oak")) ? new OakMkinitializator()
				: new SCMkInitializator();
	/*	initializator = new SCMkInitializator(); */
		System.out.println("Tests will run against ***"+initializator.getType()+"***");
		conf = MkConfigProvider.readConfig();
	}

	@Before
	public void beforeTest() throws Exception {

		mk = (new MkCollection(initializator, conf, 1)).getMicroKernels()
				.get(0);
		chronometer = new Chronometer();
	}

}
