package org.onesentence.onesentence.domain.todo.service;

import java.time.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.onesentence.onesentence.domain.chat.dto.CoordinationMessage;
import org.onesentence.onesentence.domain.dijkstra.model.Graph;
import org.onesentence.onesentence.domain.dijkstra.model.Node;
import org.onesentence.onesentence.domain.dijkstra.service.DijkstraService;
import org.onesentence.onesentence.domain.fcm.service.SchedulerService;
import org.onesentence.onesentence.domain.todo.dto.AvailableTimeSlots;
import org.onesentence.onesentence.domain.gpt.dto.GPTCallTodoRequest;
import org.onesentence.onesentence.domain.todo.dto.*;
import org.onesentence.onesentence.domain.todo.entity.Todo;
import org.onesentence.onesentence.domain.todo.entity.TodoStatus;
import org.onesentence.onesentence.domain.todo.repository.TodoJpaRepository;
import org.onesentence.onesentence.domain.todo.repository.TodoQuery;
import org.onesentence.onesentence.domain.user.entity.User;
import org.onesentence.onesentence.domain.user.repository.UserJpaRepository;
import org.onesentence.onesentence.global.exception.ExceptionStatus;
import org.onesentence.onesentence.global.exception.NotFoundException;
import org.quartz.SchedulerException;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class TodoServiceImpl implements TodoService {

	private final TodoJpaRepository todoJpaRepository;
	private final TodoQuery todoQuery;
	private final DijkstraService dijkstraService;
	private final UserJpaRepository userJpaRepository;
	private final SchedulerService schedulerService;
	private final SimpMessagingTemplate simpMessagingTemplate;

	private User checkUserByUserId(Long userId) {
		return userJpaRepository.findById(userId)
			.orElseThrow(() -> new NotFoundException(ExceptionStatus.NOT_FOUND));
	}

	@Override
	@Transactional
	public Long createTodo(TodoRequest request, Long userId) throws SchedulerException {

		User user = checkUserByUserId(userId);

		Todo todo = new Todo(request, user.getId());

		Todo savedTodo = todoJpaRepository.save(todo);

		schedulerService.setScheduler(savedTodo.getStart(), user.getFcmToken(),
			savedTodo.getTitle(), savedTodo.getId());

		return savedTodo.getId();
	}

	@Override
	@Transactional
	public Long updateTodo(TodoRequest request, Long todoId, Long userId) {

		User user = checkUserByUserId(userId);
		Todo todo = findById(todoId);

		if (!todo.getUserId().equals(user.getId())) {
			throw new IllegalArgumentException("TODO 작성자가 아닙니다.");
		}

		todo.updateTodo(request);

		return todo.getId();
	}

	@Override
	@Transactional(readOnly = true)
	public Todo findById(Long todoId) {
		return todoJpaRepository.findById(todoId).orElseThrow(() -> new NotFoundException(
			ExceptionStatus.NOT_FOUND));
	}

	@Override
	@Transactional
	public void deleteTodo(Long todoId, Long userId) {

		User user = checkUserByUserId(userId);
		Todo todo = findById(todoId);

		if (!todo.getUserId().equals(user.getId())) {
			throw new IllegalArgumentException("TODO 작성자가 아닙니다.");
		}

		todoJpaRepository.delete(todo);
	}

	@Override
	@Transactional
	public Long updateStatus(TodoStatusRequest request, Long todoId) {
		Todo todo = findById(todoId);
		if (request.getStatus().equals("진행중")) {
			todo.changeToInProgress();
		} else if (request.getStatus().equals("완료")) {
			todo.changeToDone();
		}

		return todo.getId();
	}

	@Override
	@Transactional(readOnly = true)
	public TodoResponse getTodo(Long todoId) {
		Todo todo = findById(todoId);

		return TodoResponse.from(todo);
	}

	@Override
	@Transactional(readOnly = true)
	public List<TodoResponse> getTodosByStatus(TodoStatus status, Long userId) {
		List<TodoResponse> todoResponses = new ArrayList<>();

		List<Todo> todos = todoQuery.findByStatus(status, userId);

		for (Todo todo : todos) {
			todoResponses.add(TodoResponse.from(todo));
		}

		return todoResponses;
	}

	@Override
	@Transactional(readOnly = true)
	public List<TodoResponse> getTodosByDate(LocalDate date, Long userId) {
		List<TodoResponse> todoResponses = new ArrayList<>();

		List<Todo> todos = todoQuery.findByDate(date, userId);

		for (Todo todo : todos) {
			todoResponses.add(TodoResponse.from(todo));
		}

		return todoResponses;
	}

	@Override
	@Transactional(readOnly = true)
	public List<TodoResponse> getTodosByCategory(String category, Long userId) {
		List<TodoResponse> todoResponses = new ArrayList<>();

		List<Todo> todos = todoQuery.findByCategory(category, userId);

		for (Todo todo : todos) {
			todoResponses.add(TodoResponse.from(todo));
		}

		return todoResponses;
	}

	@Override
	@Transactional(readOnly = true)
	public List<TodoResponse> getTodos(Long userId) {
		List<TodoResponse> todoResponses = new ArrayList<>();

		List<Todo> todos = todoQuery.findAll(userId);

		for (Todo todo : todos) {
			todoResponses.add(TodoResponse.from(todo));
		}

		return todoResponses;
	}

	@Transactional(readOnly = true)
	@Override
	public List<TodoResponse> getPriorities(Long userId) {
		List<TodoPriority> todoPriorities = todoQuery.calculatePriority(userId);

		List<Node> nodes = new ArrayList<>();

		for (TodoPriority todoPriority : todoPriorities) {
			nodes.add(new Node(todoPriority.getTodoId(), todoPriority.getPriorityScore()));
		}
		Graph graph = new Graph(nodes);

		for (Node node : nodes) {
			for (Node neighbor : nodes) {
				if (!node.getTodoId().equals(neighbor.getTodoId())) {
					double weight = 1.0 / (neighbor.getPriorityScore() + 1.0); // 역수 가중치 계산
					graph.addEdge(node.getTodoId(), neighbor.getTodoId(), weight);
				}
			}
		}

		List<Long> optimalOrder = dijkstraService.getOptimalOrder(graph);
		log.info("Optimal Order: {}", optimalOrder);

		List<Todo> todos = todoQuery.getTodosByOptimalOrder(optimalOrder);

		Map<Long, Todo> todoMap = todos.stream()
			.collect(Collectors.toMap(Todo::getId, todo -> todo));

		List<TodoResponse> todoResponses = new ArrayList<>();
		for (Long todoId : optimalOrder) {
			Todo todo = todoMap.get(todoId);
			if (todo != null) {
				TodoResponse todoResponse = TodoResponse.from(todo);
				todoResponses.add(todoResponse);
			}
		}
		return todoResponses;
	}


	@Override
	@Transactional
	public Long createTodoByOneSentence(GPTCallTodoRequest gptCallTodoRequest, Long userId) {

		Todo todo = Todo.builder()
			.title(gptCallTodoRequest.getTitle())
			.start(gptCallTodoRequest.getStart())
			.category(gptCallTodoRequest.getCategory())
			.status(TodoStatus.TODO)
			.end(gptCallTodoRequest.getEnd())
			.location(gptCallTodoRequest.getLocation())
			.together(gptCallTodoRequest.getTogether())
			.userId(userId)
			.build();
		Todo savedTodo = todoJpaRepository.save(todo);

		return savedTodo.getId();
	}

	@Override
	@Transactional
	public Long setInputTime(Long todoId, TodoInputTimeRequest request, Long userId) {

		User user = checkUserByUserId(userId);
		Todo todo = findById(todoId);

		if (!todo.getUserId().equals(user.getId())) {
			throw new IllegalArgumentException("TODO 작성자가 아닙니다.");
		}

		todo.setInputTime(request.getInputTime());

		return todo.getId();
	}

	@Override
	@Transactional
	public void coordinateTodo(TodoRequest request, Long userId) throws SchedulerException {

		User user = checkUserByUserId(userId);

		Todo todo = new Todo(request, user.getId());

		Todo savedTodo = todoJpaRepository.save(todo);

		schedulerService.setScheduler(savedTodo.getStart(), user.getFcmToken(),
			savedTodo.getTitle(), savedTodo.getId());

		CoordinationMessage messageDto = CoordinationMessage.builder()
			.label("answer")
			.todoId(savedTodo.getId())
			.message("아래 일정을 조율하고자 합니다.")
			.todoTitle(savedTodo.getTitle())
			.start(dateConvertToString(savedTodo.getStart()))
			.end(dateConvertToString(savedTodo.getEnd()))
			.build();

		simpMessagingTemplate.convertAndSend("/sub/chatroom/hanfinal", messageDto);
	}

	@Override
	@Transactional(readOnly = true)
	public List<TodoDate> getTodoDatesByUserId(Long todoId) {
		Todo todo = findById(todoId);

		return todoQuery.getTodoDatesByUserId(todo.getUserId());
	}

	@Transactional(readOnly = true)
	public AvailableTimeSlots findAvailableTimeSlots(Long todoId) {

		Todo targetTodo = findById(todoId);

		LocalDate targetDate = targetTodo.getStart().toLocalDate();
		LocalDateTime startDate = targetDate.atTime(LocalTime.of(10, 0));
		LocalDateTime endDate = targetDate.plusDays(2).atTime(LocalTime.of(21, 0)); // 이틀 후 21시

		List<Todo> todos = todoJpaRepository.findByUserIdAndStartBetween(targetTodo.getUserId(),
			startDate, endDate);

		List<Todo> sortedTodos = todos.stream()
			.sorted(Comparator.comparing(Todo::getStart))
			.toList();

		List<LocalDateTime> availableTimeSlots = new ArrayList<>();

		int inputTimeMinutes = targetTodo.getInputTime();

		LocalDateTime lastEndTime = startDate;

		for (Todo todo : sortedTodos) {

			// 이전 일정의 종료 시간과 현재 일정의 시작 시간 사이의 간격이 inputTime 이상인지 확인
			while (lastEndTime.plusMinutes(inputTimeMinutes).isBefore(todo.getStart()) &&
				lastEndTime.getHour() < 21) {
				availableTimeSlots.add(lastEndTime);
				if (availableTimeSlots.size() == 3) {
					return new AvailableTimeSlots(availableTimeSlots);
				}
				lastEndTime = lastEndTime.plusMinutes(inputTimeMinutes); // 3시간 추가
				// 각 날짜의 끝 시간(21:00)으로 넘어가면 다음 날 10:00으로 설정
				if (lastEndTime.getHour() >= 21) {
					lastEndTime = lastEndTime.toLocalDate().plusDays(1).atTime(10, 0);
				}
			}

			// 마지막 일정의 종료 시간을 업데이트
			lastEndTime = todo.getEnd();
		}

		// 마지막 일정 후의 빈 시간대도 확인
		while (lastEndTime.plusMinutes(inputTimeMinutes).isBefore(endDate) &&
			lastEndTime.getHour() < 21) {
			availableTimeSlots.add(lastEndTime);
			if (availableTimeSlots.size() == 3) {
				return new AvailableTimeSlots(availableTimeSlots);
			}
			lastEndTime = lastEndTime.plusMinutes(inputTimeMinutes); // 3시간 추가
			// 각 날짜의 끝 시간(21:00)으로 넘어가면 다음 날 10:00으로 설정
			if (lastEndTime.getHour() >= 21) {
				lastEndTime = lastEndTime.toLocalDate().plusDays(1).atTime(10, 0);
			}
		}

		return new AvailableTimeSlots(availableTimeSlots);

	}

	private String dateConvertToString(LocalDateTime localDateTime) {
		return localDateTime.getYear() + "년 " + localDateTime.getMonthValue() + "월 "
			+ localDateTime.getDayOfMonth() + "일 " + localDateTime.getHour() + "시 "
			+ localDateTime.getMinute() + "분";
	}
}
