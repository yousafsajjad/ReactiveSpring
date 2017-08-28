package com.example.fluxflixservice;

import java.time.Duration;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

@SpringBootApplication
public class FluxFlixServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(FluxFlixServiceApplication.class, args);
    }

    @Bean
    CommandLineRunner demo(MovieRepository movieRepository) {
        return args -> {
            movieRepository.deleteAll().subscribe(null, null,
                () -> Stream.of("A", "B", "C", "D")
                    .map(name -> new Movie(UUID.randomUUID().toString(), name, randomGenre()))
                    .forEach(m -> movieRepository.save(m).subscribe(System.out::println, null, null)));
        };
    }

    private String randomGenre() {
        String[] genres = "horror, comedy, documentary, suspense, thriller".split(",");
        return genres[new Random().nextInt(genres.length)];
    }
}

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor class MovieEvent {

    private Movie movie;

    private Date when;

    private String user;

}

@RestController
@RequestMapping("/movies") class MovieRestController {

    private final FluxFlixService fluxFlixService;

    MovieRestController(FluxFlixService fluxFlixService) {
        this.fluxFlixService = fluxFlixService;
    }

    @GetMapping
    public Flux<Movie> all() {
        return fluxFlixService.all();
    }

    @GetMapping(value = "/{id}/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<MovieEvent> events(@PathVariable String id) {
        return fluxFlixService.byId(id)
            .flatMapMany(fluxFlixService::streamStreams);
    }

    @GetMapping("/{id}")
    public Mono<Movie> findById(@PathVariable String id) {
        return fluxFlixService.byId(id);
    }
}

@Service class FluxFlixService {

    private final MovieRepository movieRepository;

    FluxFlixService(MovieRepository movieRepository) {
        this.movieRepository = movieRepository;
    }

    public Flux<MovieEvent> streamStreams(Movie movie) {
        Flux<Long> interval = Flux.interval(Duration.ofSeconds(1));
        Flux<MovieEvent> fluxEvent = Flux.fromStream(Stream.generate(() -> new MovieEvent(movie, new Date(), randomUser())));
        return Flux.zip(interval, fluxEvent).map(Tuple2::getT2);
    }

    private String randomUser() {
        String[] users = "matt, simon, claire, sarah, michelle".split(",");
        return users[new Random().nextInt(users.length)];
    }

    public Flux<Movie> all() {
        return movieRepository.findAll();

    }

    public Mono<Movie> byId(String id) {
        return movieRepository.findById(id);
    }

}

interface MovieRepository extends ReactiveCrudRepository<Movie, String> {

}

@Document
@AllArgsConstructor
@ToString
@NoArgsConstructor
@Data class Movie {

    @Id
    private String id;

    private String title, genre;

}
