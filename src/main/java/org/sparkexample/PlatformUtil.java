package org.sparkexample;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 * 
 * @author wen
 *
 */
public final class PlatformUtil {

    private static final String SLASH = "/";

    private static final String BACK_SLASH = "\\";

    /**
     * The platform enum.
     *
     */
    public static enum PlatformEnum {
        WINDOWS, UNIX, AWS;

    }

    /**
     * Private constructor.
     */
    private PlatformUtil() {
    }

    /**
     * 
     * @param path
     *            the raw path
     * @return the platform path
     */
    public static String getPlatformPath(PlatformEnum jobPlatformEnum, String path) {
        switch (jobPlatformEnum) {
        case WINDOWS:
            return path.replace(SLASH, BACK_SLASH);
        case AWS:
            return SLASH + path;
        default:
            return path;
        }
    }

    /**
     * 
     * @param path
     *            the input path
     * @return the input stream for the file at the given path
     */
    public static InputStream getPlatformInputStream(PlatformEnum jobPlatformEnum, String path) {
        try {
            InputStream fstream;
            if (jobPlatformEnum == PlatformEnum.AWS) {
                fstream = PlatformUtil.class.getResourceAsStream(path);
                System.out.println("in Platform Util getPlatformInputStream");
                System.out.println(" path = " + path);
                System.out.println(fstream);
            } else {
                fstream = new FileInputStream(path);
            }

            if (path.contains(".gz")) {
                GZIPInputStream gzipStream = new GZIPInputStream(fstream);
                return gzipStream;
            } else {
                return fstream;
            }

            // GZIPInputStream gzip = new GZIPInputStream(new
            // FileInputStream("F:/gawiki-20090614-stub-meta-history.xml.gz"));
            // BufferedReader br = new BufferedReader(new InputStreamReader(gzip));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
