package org.sparkexample;

import java.io.FileInputStream;
import java.io.InputStream;

/**
 * 
 * @author wen
 *
 */
public final class SystemParameters {

    private static final Platform JOB_PLATFORM = Platform.UNIX;

    private static final String SLASH = "/";

    private static final String BACK_SLASH = "\\";

    /**
     * The platform enum.
     *
     */
    public static enum Platform {
        WINDOWS, UNIX, AWS;

    }

    /**
     * Private constructor.
     */
    private SystemParameters() {
    }

    public static Platform getJobPlatform() {
        return JOB_PLATFORM;
    }

    /**
     * 
     * @param path
     *            the raw path
     * @return the platform path
     */
    public static String getPlatformPath(String path) {
        switch (JOB_PLATFORM) {
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
    public static InputStream getPlatformInputStream(String path) {
        try {
            InputStream fstream;
            if (JOB_PLATFORM == Platform.AWS) {
                fstream = SystemParameters.class.getResourceAsStream(path);
            } else {
                fstream = new FileInputStream(path);
            }
            return fstream;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
