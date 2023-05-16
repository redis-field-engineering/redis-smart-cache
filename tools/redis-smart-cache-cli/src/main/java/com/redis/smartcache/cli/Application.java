package com.redis.smartcache.cli;

import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@SpringBootApplication
public class Application {

	public static void main(String[] args) {
		SpringApplication app = new SpringApplication(Application.class);
		app.setLogStartupInfo(false);

		if(Arrays.stream(args).anyMatch(x-> Objects.equals(x, "-v") || Objects.equals(x, "--version"))){
			System.out.println("v0.0.1");
			System.exit(0);
		}

		if(args.length > 0 && !args[0].startsWith("-")){
			app.setBannerMode(Banner.Mode.OFF);
			if(Arrays.stream(args).anyMatch(x->Objects.equals(x,"-h") || Objects.equals(x,"--help"))){
				app.run("help", args[0]);
			}else{
				app.run(args);
			}
			System.exit(0);
		}

		if(Arrays.stream(args).anyMatch(x->Objects.equals(x,"-h") || Objects.equals(x,"--help"))){

			System.out.println("Help Smart-Cache CLI");

			System.out.println("Usage:");
			System.out.println();

			System.out.println("Options:");

			System.out.println("\t-n --hostname The Redis [h]ost.");
			System.out.println("\t-p --port The Redis [p]ort.");
			System.out.println("\t-a --password The Redis p[a]ssword.");
			System.out.println("\t-u --user The Redis [u]sername.");
			System.out.println("\t-s --application The Redis application name[s]pace.");
			System.out.println();
			System.out.println("Subcommands");
			System.out.println("\tlist-queries");
			System.out.println("\tmake-rule");

		}
		else{
			List<String> appArgs = Arrays.stream(args).collect(Collectors.toList());
			appArgs.add(0, "Interactive");

			String[] finalArgs = appArgs.toArray(new String[0]);
			app.run(finalArgs);
		}
	}

}
